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

package com.hmily.tcc.springcloud.feign;

import feign.Feign;
import feign.InvocationHandlerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * HmilyRestTemplateConfiguration.
 * @author xiaoyu
 */
@Configuration
public class HmilyRestTemplateConfiguration {

    /**
     * build feign.
     *
     * @return Feign.Builder
     */
    //@FeignClient 注解的class 初始化之后 会执行这个方法；不同的接口实现,获取不同的HmilyFeignHandler
    //@Bean 使Feign.Builder 默认使用当前方法
    //每个@FeignClient的接口 都会调用一次这个方法
    @Bean
    @Scope("prototype")
    public Feign.Builder feignBuilder() {
        return Feign.builder().requestInterceptor(new HmilyRestTemplateInterceptor()).invocationHandlerFactory(invocationHandlerFactory());
    }

    /**
     * build InvocationHandlerFactory.
     * @return InvocationHandlerFactory
     */
    //@FeignClient 注解的class 初始化之后 会执行这个方法
    @Bean
    public InvocationHandlerFactory invocationHandlerFactory() {
        return (target, dispatch) -> {
            HmilyFeignHandler handler = new HmilyFeignHandler();
            handler.setHandlers(dispatch);
            return handler;
        };
    }

}
