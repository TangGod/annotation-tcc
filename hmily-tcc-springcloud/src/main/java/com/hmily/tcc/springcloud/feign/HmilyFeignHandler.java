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

import com.hmily.tcc.annotation.Tcc;
import com.hmily.tcc.annotation.TccPatternEnum;
import com.hmily.tcc.common.bean.context.TccTransactionContext;
import com.hmily.tcc.common.bean.entity.Participant;
import com.hmily.tcc.common.bean.entity.TccInvocation;
import com.hmily.tcc.common.enums.TccActionEnum;
import com.hmily.tcc.common.enums.TccRoleEnum;
import com.hmily.tcc.core.concurrent.threadlocal.TransactionContextLocal;
import com.hmily.tcc.core.helper.SpringBeanUtils;
import com.hmily.tcc.core.service.executor.HmilyTransactionExecutor;
import feign.InvocationHandlerFactory.MethodHandler;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;

/**
 * HmilyFeignHandler.
 *
 * @author xiaoyu
 */
//实现了InvocationHandler接口  应该是根据 接口类型  来自动注入相应的HmilyFeignHandler
//不同的接口 有不同的HmilyFeignHandler
public class HmilyFeignHandler implements InvocationHandler {

    private Map<Method, MethodHandler> handlers;//项目初始化时候 会赋值  实现了@FeignClient 注解的class  当前class里的接口信息都存在这

    //每个方法都使用 HmilyFeignHandler代理
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        //Object则直接执行方法
        if (Object.class.equals(method.getDeclaringClass())) {
            return method.invoke(this, args);
        } else {
            final Tcc tcc = method.getAnnotation(Tcc.class);
            //不是 事物方法的话  则直接执行
            if (Objects.isNull(tcc)) {
                return this.handlers.get(method).invoke(args);
            }
            //开始执行事物方法
            try {
                //因为事物发起者执行的时候  初始化了事物上下文  所以这里可以直接获取
                final TccTransactionContext tccTransactionContext = TransactionContextLocal.getInstance().get();
                //方法调用的时候 都会执行这个class（目前看的代码是：事物发起者调用时 会执行begin方法）
                final HmilyTransactionExecutor hmilyTransactionExecutor =
                        SpringBeanUtils.getInstance().getBean(HmilyTransactionExecutor.class);

                //执行方法 (会进入拦截器 HmilyRestTemplateInterceptor )并把事物上下文对象 丢到head中
                final Object invoke = this.handlers.get(method).invoke(args);
                //构建一个Participant对象  属性包括：事物id、协调方法：confirm、cancel 的方法
                final Participant participant = buildParticipant(tcc, method, args, tccTransactionContext);
                //提供者
                if (tccTransactionContext.getRole() == TccRoleEnum.PROVIDER.getCode()) {
                    hmilyTransactionExecutor.registerByNested(tccTransactionContext.getTransId(),
                            participant);
                } else {//发起者、消费者、本地调用
                    hmilyTransactionExecutor.enlistParticipant(participant);
                }
                return invoke;
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                throw throwable;
            }
        }
    }

    private Participant buildParticipant(final Tcc tcc, final Method method, final Object[] args,
                                         final TccTransactionContext tccTransactionContext ) {
        if (Objects.isNull(tccTransactionContext)
                || (TccActionEnum.TRYING.getCode() != tccTransactionContext.getAction())) {
            return null;
        }
        //获取协调方法
        String confirmMethodName = tcc.confirmMethod();
        if (StringUtils.isBlank(confirmMethodName)) {
            confirmMethodName = method.getName();
        }
        String cancelMethodName = tcc.cancelMethod();
        if (StringUtils.isBlank(cancelMethodName)) {
            cancelMethodName = method.getName();
        }
        final Class<?> declaringClass = method.getDeclaringClass();
        TccInvocation confirmInvocation = new TccInvocation(declaringClass, confirmMethodName, method.getParameterTypes(), args);
        TccInvocation cancelInvocation = new TccInvocation(declaringClass, cancelMethodName, method.getParameterTypes(), args);
        //封装调用点
        return new Participant(tccTransactionContext.getTransId(), confirmInvocation, cancelInvocation);
    }

    /**
     * set handlers.
     * @param handlers handlers
     */
    public void setHandlers(final Map<Method, MethodHandler> handlers) {
        this.handlers = handlers;
    }

}
