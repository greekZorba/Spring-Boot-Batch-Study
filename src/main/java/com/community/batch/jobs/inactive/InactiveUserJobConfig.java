package com.community.batch.jobs.inactive;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.repository.UserRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

@AllArgsConstructor
@Configuration
public class InactiveUserJobConfig {

    private final static int CHUNK_SIZE = 15;

    private final EntityManagerFactory entityManagerFactory;

    private UserRepository userRepository;

    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory,
                                Step inactiveJobStep) {
        return jobBuilderFactory.get("inactiveUserJob")
                .preventRestart()
                .start(inactiveJobStep)
                .build();
    }

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
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory,
                                ListItemReader<User> inactiveUserReader) {
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User> chunk(CHUNK_SIZE)
                .reader(inactiveUserReader)
                .processor(inactiveUserProcessor())
                .writer(inactiveUserWriter())
                .build();
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

    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserReader(UserRepository userRepository,
                                                   @Value("#{jobParameteres[nowDate]}") Date nowDate) {
        LocalDateTime now = LocalDateTime.ofInstant(nowDate.toInstant(), ZoneId.systemDefault());

        List<User> inactiveUsers =
                userRepository.findByUpdatedDateBeforeAndStatusEquals(
                        now.minusYears(1), UserStatus.ACTIVE);
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
