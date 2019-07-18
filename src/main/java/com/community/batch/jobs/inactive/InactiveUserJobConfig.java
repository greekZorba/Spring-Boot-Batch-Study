package com.community.batch.jobs.inactive;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.Grade;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.inactive.listener.InactiveChunkListener;
import com.community.batch.jobs.inactive.listener.InactiveIJobListener;
import com.community.batch.jobs.inactive.listener.InactiveStepListener;
import com.community.batch.repository.UserRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
@AllArgsConstructor
@Configuration
public class InactiveUserJobConfig {

    private final static int CHUNK_SIZE = 15;

    private final EntityManagerFactory entityManagerFactory;

    private UserRepository userRepository;

    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory
                                , InactiveIJobListener inactiveIJobListener
//                                , Flow multiFlow
                                , Step partitionerStep) {
        return jobBuilderFactory.get("inactiveUserJob")
                .preventRestart()
                .listener(inactiveIJobListener)
//                .start(multiFlow)
                .start(partitionerStep)
                .build();
    }

    @Bean
    @JobScope
    public Step partitionerStep(StepBuilderFactory stepBuilderFactory
                    , Step inactiveJobStep) {
        return stepBuilderFactory
                .get("partitionerStep")
                .partitioner("partitionerStep", new InactiveUserPartitioner())
                .gridSize(5)
                .step(inactiveJobStep)
                .taskExecutor(taskExecutor())
                .build();
    }

//    @Bean
//    public Flow multiFlow(Step inactiveJobStep) {
//        Flow flows[] = new Flow[5];
//        IntStream.range(0, flows.length).forEach(i -> flows[i] =
//                new FlowBuilder<Flow>("MultiFlow"+i).from(inactiveJobFlow(
//                        inactiveJobStep)).end());
//
//        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("MultiFlowTest");
//        return flowBuilder
//                .split(taskExecutor())
//                .add(flows)
//                .build();
//     }


//    @Bean
//    public Flow inactiveJobFlow(Step inactiveJobStep) {
//        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("inactiveJobFlow");
//        return flowBuilder
//                .start(new InactiveJobExecutionDecider())
//                .on(FlowExecutionStatus.FAILED.getName()).end()
//                .on(FlowExecutionStatus.COMPLETED.getName()).to(inactiveJobStep)
//                .end();
//    }



//    @Bean
//    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory,
//                                JpaPagingItemReader<User> inactiveUserJpaReader) {
//        return stepBuilderFactory.get("inactiveUserStep")
//                .<User, User> chunk(CHUNK_SIZE)
//                .reader(inactiveUserJpaReader)
//                .processor(inactiveUserProcessor())
//                .writer(inactiveUserWriter())
//                .build();
//    }
    @Bean
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory
                            , InactiveStepListener inactiveStepListener
                            , InactiveChunkListener inactiveChunkListener
                            , ListItemReader<User> inactiveUserReader
                            , TaskExecutor taskExecutor) {
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User> chunk(CHUNK_SIZE)
                .reader(inactiveUserReader)
                .processor(inactiveUserProcessor())
                .writer(inactiveUserWriter())
                .listener(inactiveChunkListener)
                .listener(inactiveStepListener)
                .taskExecutor(taskExecutor)
                .throttleLimit(2)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("Batch_Task");
    }

//    @Bean(destroyMethod = "")
//    @StepScope
//    public JpaPagingItemReader<User> inactiveUserJpaReader() {
//        JpaPagingItemReader<User> jpaPagingItemReader = new JpaPagingItemReader() {
//            @Override
//            public int getPage() {
//                return 0;
//            }
//        };
//        jpaPagingItemReader.setQueryString("select u from User as u where" +
//                "u.updatedDate < :updatedDate and u.status = :status");
//
//        Map<String, Object> map = new HashMap<>();
//        LocalDateTime now = LocalDateTime.now();
//        map.put("updatedDate", now.minusYears(1));
//        map.put("status", UserStatus.ACTIVE);
//
//        jpaPagingItemReader.setParameterValues(map);
//        jpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
//        return jpaPagingItemReader;
//    }

//    @Bean
//    @StepScope
//    public ListItemReader<User> inactiveUserReader(UserRepository userRepository,
//                                                   @Value("#{jobParameters[nowDate]}") Date nowDate) {
//        LocalDateTime now = LocalDateTime.ofInstant(nowDate.toInstant(), ZoneId.systemDefault());
//
//        List<User> inactiveUsers =
//                userRepository.findByUpdatedDateBeforeAndStatusEquals(
//                        now.minusYears(1), UserStatus.ACTIVE);
//        return new ListItemReader<>(inactiveUsers);
//    }

    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserReader(UserRepository userRepository,
                                                   @Value("#{stepExecutionContext[grade]}") String grade) {
        log.info(Thread.currentThread().getName());
        List<User> inactiveUsers =
                userRepository.findByUpdatedDateBeforeAndStatusEqualsAndGradeEquals(
                        LocalDateTime.now().minusYears(1), UserStatus.ACTIVE,
                        Grade.valueOf(grade));
        return new ListItemReader<>(inactiveUsers);
    }

    private ItemProcessor<User, User> inactiveUserProcessor() {
        // return User::setInactive;
        return new ItemProcessor<User, User>() {
            @Override
            public User process(User user) throws Exception {
                return user.setInactive();
            }
        };
    }

    private JpaItemWriter<User> inactiveUserWriter() {
        JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }

}
