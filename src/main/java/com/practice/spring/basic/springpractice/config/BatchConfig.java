package com.practice.spring.basic.springpractice.config;

import com.practice.spring.basic.springpractice.model.UserRoles;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableBatchProcessing
public class BatchConfig
{
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job readCSVFilesJob() {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }

    @Bean
    public Step step1() {
        System.out.println("step started");
        return stepBuilderFactory.get("step1").<UserRoles, UserRoles>chunk(5)
                .reader(reader())
                .writer(writer())
                .build();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Bean
    public FlatFileItemReader<UserRoles> reader()
    {
        //Create reader instance
        FlatFileItemReader<UserRoles> reader = new FlatFileItemReader<UserRoles>();

        //Set input file location
        reader.setResource(new FileSystemResource("input/inputData.csv"));

        //Set number of lines to skips. Use it if file has header rows.
        reader.setLinesToSkip(1);

        //Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper() {
            {
                //3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[] { "id", "userId", "roleName","roleDescription","ica","status"
                        ,"createdTimestamp"
                                ,"lastUpdatedTimestamp"});
                    }
                });
                //Set values in UserRoles class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<UserRoles>() {
                    {
                        setTargetType(UserRoles.class);
                    }
                });
            }
        });
        return reader;
    }
    private Resource outputResource = new FileSystemResource("output/outputData.csv");

    @Bean
    public FlatFileItemWriter<UserRoles> writer()
    {
        System.out.println("writer started");

        //Create writer instance
        FlatFileItemWriter<UserRoles> writer = new FlatFileItemWriter<>();

        //Set output file location
        writer.setResource(outputResource);

        //All job repetitions should "append" to same output file
        writer.setAppendAllowed(true);

        //Name field values sequence based on object properties
        writer.setLineAggregator(new DelimitedLineAggregator<UserRoles>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<UserRoles>() {
                    {
                        setNames(new String[] { "id", "userId", "roleName","roleDescription","ica","status","createdTimestamp"
                        ,"lastUpdatedTimestamp"});
                    }
                });
            }
        });
        System.out.println("I m in writer");
        return writer;
    }


    @Bean
    ItemReader<UserRoles> databaseCsvItemReader(DataSource dataSource) {
        JdbcPagingItemReader<UserRoles> databaseReader = new JdbcPagingItemReader<>();
        System.out.println("reader started");
        databaseReader.setDataSource(dataSource);
        databaseReader.setPageSize(1);

        PagingQueryProvider queryProvider = createQueryProvider();
        databaseReader.setQueryProvider(queryProvider);

        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(UserRoles.class));
        System.out.println("I m in reader");
        return databaseReader;
    }
    private PagingQueryProvider createQueryProvider() {
        H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();
        queryProvider.setSelectClause("SELECT id,user_Id,role_Name,role_Description,ica,status,created_Timestamp,last_Updated_Timestamp");
        queryProvider.setFromClause("FROM User_Roles");
        //queryProvider.setWhereClause("WHERE status='ACTIVE' AND role_Name='TestRole'");
        queryProvider.setSortKeys(sortById());
        return queryProvider;
    }
    private Map<String, Order> sortById() {
        Map<String, Order> sortConfiguration = new HashMap<>();
        sortConfiguration.put("id", Order.ASCENDING);
        return sortConfiguration;
    }

    public DataSource getDataSource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.h2.Driver");
        dataSourceBuilder.url("jdbc:h2:mem:tests");
        dataSourceBuilder.username("sa");
        dataSourceBuilder.password("sa");
        return dataSourceBuilder.build();
    }

}