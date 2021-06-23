package org.gbif.validator.ws.config;

import org.gbif.mybatis.type.UriTypeHandler;
import org.gbif.mybatis.type.UuidTypeHandler;
import org.gbif.registry.persistence.mapper.CommentMapper;
import org.gbif.registry.persistence.mapper.ContactMapper;
import org.gbif.registry.persistence.mapper.EndpointMapper;
import org.gbif.registry.persistence.mapper.IdentifierMapper;
import org.gbif.registry.persistence.mapper.MachineTagMapper;
import org.gbif.registry.persistence.mapper.OrganizationMapper;
import org.gbif.registry.persistence.mapper.TagMapper;
import org.gbif.registry.persistence.mapper.UserMapper;
import org.gbif.registry.persistence.mapper.surety.ChallengeCodeMapper;

import java.net.URI;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SecurityDataSourceConfiguration {

  @Bean(name = "registryDatasourceProperties")
  @ConfigurationProperties("registry.datasource")
  public DataSourceProperties registryDataSourceProperties() {
    return new DataSourceProperties();
  }

  @Bean(name = "registryDataSource")
  @ConfigurationProperties("registry.datasource.hikari")
  public HikariDataSource registryDataSource() {
    return registryDataSourceProperties()
      .initializeDataSourceBuilder()
      .type(HikariDataSource.class)
      .build();
  }

  @Bean(name = "registrySqlSessionFactory")
  public SqlSessionFactoryBean registryServiceSqlSessionFactory(@Qualifier("registryDataSource") HikariDataSource dataSource) throws Exception {
    SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
    bean.setDataSource(dataSource);
    //Configure the location of the Mybatis XML file
    org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
    configuration.getTypeHandlerRegistry().register(UUID.class, UuidTypeHandler.class);
    configuration.getTypeHandlerRegistry().register(URI.class, UriTypeHandler.class);
    configuration.getTypeHandlerRegistry().register("org.gbif.registry.persistence.handler");
    configuration.setMapUnderscoreToCamelCase(true);
    configuration.addMapper(UserMapper.class);
    configuration.addMapper(ChallengeCodeMapper.class);
    configuration.addMapper(OrganizationMapper.class);
    bean.setConfiguration(configuration);
    return bean;
  }

  @Bean
  public MapperFactoryBean<UserMapper> userMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<UserMapper> factoryBean = new MapperFactoryBean<>(UserMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<ChallengeCodeMapper> challengeCodeMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<ChallengeCodeMapper> factoryBean = new MapperFactoryBean<>(ChallengeCodeMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<OrganizationMapper> organizationMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<OrganizationMapper> factoryBean = new MapperFactoryBean<>(OrganizationMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<TagMapper> tagMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<TagMapper> factoryBean = new MapperFactoryBean<>(TagMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<ContactMapper> contactMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<ContactMapper> factoryBean = new MapperFactoryBean<>(ContactMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<IdentifierMapper> identifierMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<IdentifierMapper> factoryBean = new MapperFactoryBean<>(IdentifierMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<EndpointMapper> endpointMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<EndpointMapper> factoryBean = new MapperFactoryBean<>(EndpointMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<MachineTagMapper> machineTagMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<MachineTagMapper> factoryBean = new MapperFactoryBean<>(MachineTagMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<CommentMapper> commentMapper(@Qualifier("registrySqlSessionFactory") final SqlSessionFactoryBean sqlSessionFactoryBean) throws Exception {
    MapperFactoryBean<CommentMapper> factoryBean = new MapperFactoryBean<>(CommentMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }
}
