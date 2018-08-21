/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hmily.tcc.core.service.impl;

import com.hmily.tcc.common.config.TccConfig;
import com.hmily.tcc.common.enums.RepositorySupportEnum;
import com.hmily.tcc.common.enums.SerializeEnum;
import com.hmily.tcc.common.serializer.KryoSerializer;
import com.hmily.tcc.common.serializer.ObjectSerializer;
import com.hmily.tcc.common.utils.LogUtil;
import com.hmily.tcc.common.utils.ServiceBootstrap;
import com.hmily.tcc.core.coordinator.CoordinatorService;
import com.hmily.tcc.core.disruptor.publisher.HmilyTransactionEventPublisher;
import com.hmily.tcc.core.helper.SpringBeanUtils;
import com.hmily.tcc.core.service.HmilyInitService;
import com.hmily.tcc.core.spi.CoordinatorRepository;
import com.hmily.tcc.core.spi.repository.JdbcCoordinatorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

/**
 * hmily tcc init service.
 * @author xiaoyu
 */
@Service("tccInitService")
public class HmilyInitServiceImpl implements HmilyInitService {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HmilyInitServiceImpl.class);

    private final CoordinatorService coordinatorService;

    private final HmilyTransactionEventPublisher hmilyTransactionEventPublisher;

    @Autowired
    public HmilyInitServiceImpl(final CoordinatorService coordinatorService, final HmilyTransactionEventPublisher hmilyTransactionEventPublisher) {
        this.coordinatorService = coordinatorService;
        this.hmilyTransactionEventPublisher = hmilyTransactionEventPublisher;
    }

    /**
     * hmily initialization.
     *
     * @param tccConfig {@linkplain TccConfig}
     */
    @Override
    public void initialization(final TccConfig tccConfig) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> LOGGER.info("hmily shutdown now")));
        try {
            //加载spi配置，把spi的配置注入成spring的bean 方便后续的使用
            //就是框架所支持的序列化，存储方式
            loadSpiSupport(tccConfig);

            //未知
            hmilyTransactionEventPublisher.start(tccConfig.getBufferSize());
            //协调者？？
            coordinatorService.start(tccConfig);
        } catch (Exception ex) {
            LogUtil.error(LOGGER, " hmily init exception:{}", ex::getMessage);
            //非正常关闭
            System.exit(1);
        }
        LogUtil.info(LOGGER, () -> "hmily init success!");
    }

    /**
     * load spi.
     *
     * @param tccConfig  {@linkplain TccConfig}
     */
    private void loadSpiSupport(final TccConfig tccConfig) {
        //spi  serialize
        //设置序列化方式,如果不设定值  默认的则为 ： KRYO 的序列化
        final SerializeEnum serializeEnum = SerializeEnum.acquire(tccConfig.getSerializer());
        //加载所有序列化类
        final ServiceLoader<ObjectSerializer> objectSerializers = ServiceBootstrap.loadAll(ObjectSerializer.class);
        //获取序列化方式
        final ObjectSerializer serializer = StreamSupport.stream(objectSerializers.spliterator(), false)
                //过滤序列化方式为：  serializeEnum
                .filter(objectSerializer -> Objects.equals(objectSerializer.getScheme(), serializeEnum.getSerialize()))
                //在spi的class中没查找到与serializeEnum对应的序列化方式，则默认为 KRYO
                .findFirst().orElse(new KryoSerializer());
        //spi  repository
        //设置存储方式,如果不设定值  默认的则为 ： JDBC的方式
        final RepositorySupportEnum repositorySupportEnum = RepositorySupportEnum.acquire(tccConfig.getRepositorySupport());
        //加载所有存储类
        final ServiceLoader<CoordinatorRepository> recoverRepositories = ServiceBootstrap.loadAll(CoordinatorRepository.class);
        //获取存储方式
        final CoordinatorRepository repository = StreamSupport.stream(recoverRepositories.spliterator(), false)
                //过滤存储方式为：  repositorySupportEnum
                .filter(recoverRepository -> Objects.equals(recoverRepository.getScheme(), repositorySupportEnum.getSupport()))
                //在spi的class中没查找到与repositorySupportEnum对应的序列化方式，则默认为 JDBC
                .findFirst().orElse(new JdbcCoordinatorRepository());
        //setter
        repository.setSerializer(serializer);
        //将CoordinatorRepository实现注入到spring容器
        SpringBeanUtils.getInstance().registerBean(CoordinatorRepository.class.getName(), repository);
    }
}
