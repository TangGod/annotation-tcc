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

package com.hmily.tcc.core.service.executor;

import com.google.common.collect.Lists;
import com.hmily.tcc.annotation.Tcc;
import com.hmily.tcc.annotation.TccPatternEnum;
import com.hmily.tcc.common.bean.context.TccTransactionContext;
import com.hmily.tcc.common.bean.entity.Participant;
import com.hmily.tcc.common.bean.entity.TccInvocation;
import com.hmily.tcc.common.bean.entity.TccTransaction;
import com.hmily.tcc.common.enums.EventTypeEnum;
import com.hmily.tcc.common.enums.TccActionEnum;
import com.hmily.tcc.common.enums.TccRoleEnum;
import com.hmily.tcc.common.exception.TccRuntimeException;
import com.hmily.tcc.common.utils.LogUtil;
import com.hmily.tcc.core.cache.TccTransactionCacheManager;
import com.hmily.tcc.core.concurrent.threadlocal.TransactionContextLocal;
import com.hmily.tcc.core.disruptor.publisher.HmilyTransactionEventPublisher;
import com.hmily.tcc.core.helper.SpringBeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * this is transaction manager.
 *
 * @author xiaoyu
 */
@Component
@SuppressWarnings("unchecked")
public class HmilyTransactionExecutor {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HmilyTransactionExecutor.class);

    /**
     * transaction save threadLocal.
     */
    private static final ThreadLocal<TccTransaction> CURRENT = new ThreadLocal<>();

    private HmilyTransactionEventPublisher hmilyTransactionEventPublisher;

    @Autowired
    public HmilyTransactionExecutor(final HmilyTransactionEventPublisher hmilyTransactionEventPublisher) {
        this.hmilyTransactionEventPublisher = hmilyTransactionEventPublisher;
    }

    public static ThreadLocal<TccTransaction> instance() {
        return CURRENT;
    }

    /**
     * transaction begin.
     *
     * 该方法为发起方第一次调用
     * 也是tcc事务的入口
     *
     * @param point cut point.
     * @return TccTransaction
     */
    public TccTransaction begin(final ProceedingJoinPoint point) {
        LogUtil.debug(LOGGER, () -> "......hmily transaction！start....");
        //build tccTransaction
        //构建tccTransaction信息
        final TccTransaction tccTransaction = buildTccTransaction(point, TccRoleEnum.START.getCode(), null);
        //save tccTransaction in threadLocal
        //缓存当前事物信息
        CURRENT.set(tccTransaction);
        //publishEvent
        //这是做什么的？？
        hmilyTransactionEventPublisher.publishEvent(tccTransaction, EventTypeEnum.SAVE.getCode());
        //set TccTransactionContext this context transfer remote
        //设置tcc事务上下文，这个类会传递给远端
        TccTransactionContext context = new TccTransactionContext();
        //set action is try
        context.setAction(TccActionEnum.TRYING.getCode());//设置执行动作为try
        context.setTransId(tccTransaction.getTransId());//设置事务id
        context.setRole(TccRoleEnum.START.getCode());//发起者
        //当前角色为，事物发起者的时候，初始化TransactionContextLocal 的 context
        TransactionContextLocal.getInstance().set(context);
        return tccTransaction;
    }


    /**
     * this is Participant transaction begin.
     *
     * @param context transaction context.
     * @param point   cut point
     * @return TccTransaction
     */
    public TccTransaction beginParticipant(final TccTransactionContext context, final ProceedingJoinPoint point) {
        LogUtil.debug(LOGGER, "...Participant hmily transaction ！start..：{}", context::toString);
        final TccTransaction tccTransaction = buildTccTransaction(point, TccRoleEnum.PROVIDER.getCode(), context.getTransId());
        //cache by guava
        TccTransactionCacheManager.getInstance().cacheTccTransaction(tccTransaction);
        //publishEvent
        hmilyTransactionEventPublisher.publishEvent(tccTransaction, EventTypeEnum.SAVE.getCode());
        //Nested transaction support
        context.setRole(TccRoleEnum.PROVIDER.getCode());
        TransactionContextLocal.getInstance().set(context);
        return tccTransaction;
    }

    /**
     * update transaction status by disruptor.
     *
     * @param tccTransaction {@linkplain TccTransaction}
     */
    public void updateStatus(final TccTransaction tccTransaction) {
        hmilyTransactionEventPublisher.publishEvent(tccTransaction, EventTypeEnum.UPDATE_STATUS.getCode());
    }

    /**
     * delete transaction by disruptor.
     *
     * @param tccTransaction {@linkplain TccTransaction}
     */
    public void deleteTransaction(final TccTransaction tccTransaction) {
        hmilyTransactionEventPublisher.publishEvent(tccTransaction, EventTypeEnum.DELETE.getCode());
    }

    /**
     * update Participant in transaction by disruptor.
     *
     * @param tccTransaction {@linkplain TccTransaction}
     */
    public void updateParticipant(final TccTransaction tccTransaction) {
        hmilyTransactionEventPublisher.publishEvent(tccTransaction, EventTypeEnum.UPDATE_PARTICIPANT.getCode());
    }

    /**
     * acquired by threadLocal.
     *
     * @return {@linkplain TccTransaction}
     */
    public TccTransaction getCurrentTransaction() {
        return CURRENT.get();
    }

    /**
     * add participant.
     *
     * @param participant {@linkplain Participant}
     */
    public void enlistParticipant(final Participant participant) {
        if (Objects.isNull(participant)) {
            return;
        }
        Optional.ofNullable(getCurrentTransaction())
                .ifPresent(c -> {
                    //保存参与者
                    c.registerParticipant(participant);
                    updateParticipant(c);
                });
    }

    /**
     * when nested transaction add participant.
     *
     * @param transId     key
     * @param participant {@linkplain Participant}
     */
    public void registerByNested(final String transId, final Participant participant) {
        if (Objects.isNull(participant)) {
            return;
        }
        final TccTransaction tccTransaction = TccTransactionCacheManager.getInstance().getTccTransaction(transId);
        Optional.ofNullable(tccTransaction)
                .ifPresent(c -> {
                    c.registerParticipant(participant);
                    updateParticipant(c);
                });
    }

    /**
     * Call the confirm method and basically if the initiator calls here call the remote or the original method
     * However, the context sets the call confirm
     * The remote service calls the confirm method.
     *
     * @param currentTransaction {@linkplain TccTransaction}
     * @throws TccRuntimeException ex
     */
    public void confirm(final TccTransaction currentTransaction) throws TccRuntimeException {
        LogUtil.debug(LOGGER, () -> "tcc confirm .......！start");
        if (Objects.isNull(currentTransaction) || CollectionUtils.isEmpty(currentTransaction.getParticipants())) {
            return;
        }
        currentTransaction.setStatus(TccActionEnum.CONFIRMING.getCode());
        updateStatus(currentTransaction);
        final List<Participant> participants = currentTransaction.getParticipants();
        List<Participant> failList = Lists.newArrayListWithCapacity(participants.size());
        boolean success = true;
        if (CollectionUtils.isNotEmpty(participants)) {
            for (Participant participant : participants) {
                try {
                    TccTransactionContext context = new TccTransactionContext();
                    context.setAction(TccActionEnum.CONFIRMING.getCode());
                    context.setTransId(participant.getTransId());
                    TransactionContextLocal.getInstance().set(context);
                    executeParticipantMethod(participant.getConfirmTccInvocation());
                } catch (Exception e) {
                    LogUtil.error(LOGGER, "execute confirm :{}", () -> e);
                    success = false;
                    failList.add(participant);
                }
            }
            executeHandler(success, currentTransaction, failList);
        }
    }

    /**
     * cancel transaction.
     *
     * @param currentTransaction {@linkplain TccTransaction}
     */
    public void cancel(final TccTransaction currentTransaction) {
        LogUtil.debug(LOGGER, () -> "tcc cancel ...........start!");
        if (Objects.isNull(currentTransaction) || CollectionUtils.isEmpty(currentTransaction.getParticipants())) {
            return;
        }
        //if cc pattern，can not execute cancel
        if (currentTransaction.getStatus() == TccActionEnum.TRYING.getCode()
                && Objects.equals(currentTransaction.getPattern(), TccPatternEnum.CC.getCode())) {
            deleteTransaction(currentTransaction);
            return;
        }
        final List<Participant> participants = filterPoint(currentTransaction);
        currentTransaction.setStatus(TccActionEnum.CANCELING.getCode());
        //update cancel
        updateStatus(currentTransaction);
        boolean success = true;
        List<Participant> failList = Lists.newArrayListWithCapacity(participants.size());
        if (CollectionUtils.isNotEmpty(participants)) {
            for (Participant participant : participants) {
                try {
                    TccTransactionContext context = new TccTransactionContext();
                    context.setAction(TccActionEnum.CANCELING.getCode());
                    context.setTransId(participant.getTransId());
                    TransactionContextLocal.getInstance().set(context);
                    executeParticipantMethod(participant.getCancelTccInvocation());
                } catch (Throwable e) {
                    LogUtil.error(LOGGER, "execute cancel ex:{}", () -> e);
                    success = false;
                    failList.add(participant);
                }
            }
            executeHandler(success, currentTransaction, failList);
        }
    }

    private void executeHandler(final boolean success, final TccTransaction currentTransaction, final List<Participant> failList) {
        TransactionContextLocal.getInstance().remove();
        TccTransactionCacheManager.getInstance().removeByKey(currentTransaction.getTransId());
        if (success) {
            //deleteTransaction(currentTransaction);
        } else {
            currentTransaction.setParticipants(failList);
            updateParticipant(currentTransaction);
            throw new TccRuntimeException(failList.toString());
        }
    }

    private List<Participant> filterPoint(final TccTransaction currentTransaction) {
        final List<Participant> participants = currentTransaction.getParticipants();
        if (CollectionUtils.isNotEmpty(participants)) {
            if (currentTransaction.getStatus() == TccActionEnum.TRYING.getCode()
                    && currentTransaction.getRole() == TccRoleEnum.START.getCode()) {
                return participants.stream()
                        .limit(participants.size())
                        .filter(Objects::nonNull).collect(Collectors.toList());
            }
        }
        return participants;
    }

    private void executeParticipantMethod(final TccInvocation tccInvocation) throws Exception {
        if (Objects.nonNull(tccInvocation)) {
            final Class clazz = tccInvocation.getTargetClass();
            final String method = tccInvocation.getMethodName();
            final Object[] args = tccInvocation.getArgs();
            final Class[] parameterTypes = tccInvocation.getParameterTypes();
            final Object bean = SpringBeanUtils.getInstance().getBean(clazz);
            MethodUtils.invokeMethod(bean, method, args, parameterTypes);
        }
    }

    /**
     * jude transaction is running.
     *
     * @return true
     */
    public boolean isBegin() {
        return CURRENT.get() != null;
    }

    public void remove() {
        CURRENT.remove();
    }

    private TccTransaction buildTccTransaction(final ProceedingJoinPoint point, final int role, final String transId) {
        TccTransaction tccTransaction;
        //如果transId不为空
        if (StringUtils.isNoneBlank(transId)) {
            tccTransaction = new TccTransaction(transId);
        } else {
            tccTransaction = new TccTransaction();
        }
        //状态：开始执行try
        tccTransaction.setStatus(TccActionEnum.PRE_TRY.getCode());
        //路由：发起者
        tccTransaction.setRole(role);
        MethodSignature signature = (MethodSignature) point.getSignature();
        //获取要执行的方法
        Method method = signature.getMethod();
        //获取执行方法的class
        Class<?> clazz = point.getTarget().getClass();
        //获取执行方法的参数值数组
        Object[] args = point.getArgs();
        //获取tcc注解
        final Tcc tcc = method.getAnnotation(Tcc.class);
        //事物模式,默认为 TCC： try,confirm,cancel模式
        final TccPatternEnum pattern = tcc.pattern();
        //目标实现类的包名+类名
        tccTransaction.setTargetClass(clazz.getName());
        //要执行的方法名
        tccTransaction.setTargetMethod(method.getName());
        //模式的code
        tccTransaction.setPattern(pattern.getCode());
        //confirm方法信息类(可使用这个class 来反射执行confirm方法)
        TccInvocation confirmInvocation = null;
        //confirm方法名
        String confirmMethodName = tcc.confirmMethod();
        //cancel方法
        String cancelMethodName = tcc.cancelMethod();
        //写了confirm方法,则构建实体
        if (StringUtils.isNoneBlank(confirmMethodName)) {
            confirmInvocation = new TccInvocation(clazz, confirmMethodName, method.getParameterTypes(), args);
        }
        //cancel方法信息类(可使用这个class 来反射执行cancel方法)
        TccInvocation cancelInvocation = null;
        //写了cancel方法,则构建实体
        if (StringUtils.isNoneBlank(cancelMethodName)) {
            cancelInvocation = new TccInvocation(clazz, cancelMethodName, method.getParameterTypes(), args);
        }
        //构造一个参与者,没有 confirm和cancel方法的  对应的属性,则为空
        final Participant participant = new Participant(tccTransaction.getTransId(), confirmInvocation, cancelInvocation);
        //注册一个参与者
        tccTransaction.registerParticipant(participant);
        return tccTransaction;
    }

}
