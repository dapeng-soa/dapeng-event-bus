package com.today.eventbus.spring;

import com.today.eventbus.ConsumerEndpoint;
import com.today.eventbus.annotation.KafkaConsumer;
import com.today.eventbus.annotation.KafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.*;

/**
 * 描述: MsgAnnotationBeanPostProcessor bean 后处理器，扫描自定义注解 @KafkaListener
 *
 * @author hz.lei
 * @date 2018年03月01日 下午9:36
 */
public class MsgAnnotationBeanPostProcessor implements BeanPostProcessor, Ordered, SmartInitializingSingleton {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaListenerRegistrar registrar = new KafkaListenerRegistrar();

    /**
     * 所有单例 bean 初始化完成后，调用此方法
     */
    @Override
    public void afterSingletonsInstantiated() {
        this.registrar.afterPropertiesSet();
    }


    /**
     * 实例化及依赖注入完成后、在任何初始化代码（比如配置文件中的init-method）调用之前调用
     *
     * @param bean
     * @param beanName
     * @return
     * @throws BeansException
     */
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    /**
     * 实例化及依赖注入完成后、在任何初始化代码（比如配置文件中的init-method）调用之后调用
     *
     * @param bean
     * @param beanName
     * @return
     * @throws BeansException
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        logger.debug("access to postProcessAfterInitialization bean {}, beanName {}", bean, beanName);

        Class<?> targetClass = AopUtils.getTargetClass(bean);
        //获取类上是否有注解 @KafkaConsumer
        Optional<KafkaConsumer> kafkaConsumer = findListenerAnnotations(targetClass);
        //类上是否有注解
        final boolean hasKafkaConsumer = kafkaConsumer.isPresent();

        if (hasKafkaConsumer) {
            //方法列表
            final List<Method> multiMethods = new ArrayList<>();

            Map<Method, Set<KafkaListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                    (MethodIntrospector.MetadataLookup<Set<KafkaListener>>) method -> {
                        Set<KafkaListener> listenerMethods = findListenerAnnotations(method);
                        return (!listenerMethods.isEmpty() ? listenerMethods : null);
                    });


            if (annotatedMethods.isEmpty()) {
                throw new IllegalArgumentException("@KafkaConsumer found on class type , " +
                        "but no @KafkaListener found on the method ,please set it on the method");
            } else {
                // Non-empty set of methods
                for (Map.Entry<Method, Set<KafkaListener>> entry : annotatedMethods.entrySet()) {
                    Method method = entry.getKey();
                    for (KafkaListener listener : entry.getValue()) {
                        // process annotation information
                        processKafkaListener(kafkaConsumer.get(), listener, method, bean, beanName);
                    }
                }
            }

            if (this.logger.isDebugEnabled()) {
                this.logger.debug(annotatedMethods.size() + " @KafkaListener methods processed on bean '"
                        + beanName + "': " + annotatedMethods);
            }
        } else {
            this.logger.info("No @KafkaConsumer annotations found on bean type: " + bean.getClass());
        }
        return bean;
    }


    /**
     * 扫描 bean 类上 是否有注解 @KafkaConsumer,只有有此注解才说明 是kafka message 消费者
     */
    private Optional<KafkaConsumer> findListenerAnnotations(Class<?> clazz) {
        KafkaConsumer ann = AnnotationUtils.findAnnotation(clazz, KafkaConsumer.class);
        return Optional.ofNullable(ann);
    }

    /**
     * 扫描bean 方法上 是否有注解 @KafkaListener
     *
     * @param method
     * @return
     */
    private Set<KafkaListener> findListenerAnnotations(Method method) {
        Set<KafkaListener> listeners = new HashSet<KafkaListener>();
        KafkaListener ann = AnnotationUtils.findAnnotation(method, KafkaListener.class);
        if (ann != null) {
            listeners.add(ann);
        }

        return listeners;
    }


    /**
     * 处理有 @KafkaListener 注解的 方法上注解元信息，封装成 consumerEndpoint，注册
     *
     * @param consumer
     * @param listener
     * @param method
     * @param bean
     * @param beanName
     */
    protected void processKafkaListener(KafkaConsumer consumer, KafkaListener listener, Method method, Object bean, String beanName) {
        Method methodToUse = checkProxy(method, bean);
        ConsumerEndpoint endpoint = new ConsumerEndpoint();
        endpoint.setMethod(methodToUse);
        endpoint.setBean(bean);
        endpoint.setParameterTypes(Arrays.asList(method.getParameterTypes()));
        // class annotation information
        endpoint.setGroupId(consumer.groupId());
        endpoint.setTopic(consumer.topic());
        endpoint.setKafkaHostKey(consumer.kafkaHostKey());
        // method annotation information
        endpoint.setSerializer(listener.serializer());

        this.registrar.registerEndpoint(endpoint);
    }

    /**
     * 获取目标方法，如果是代理的，获得其目标方法
     *
     * @param methodArg
     * @param bean
     * @return
     */
    private Method checkProxy(Method methodArg, Object bean) {
        Method method = methodArg;
        if (AopUtils.isJdkDynamicProxy(bean)) {
            try {
                // Found a @KafkaListener method on the target class for this JDK proxy ->
                // is it also present on the proxy itself?
                method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
                Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
                for (Class<?> iface : proxiedInterfaces) {
                    try {
                        method = iface.getMethod(method.getName(), method.getParameterTypes());
                        break;
                    } catch (NoSuchMethodException noMethod) {
                    }
                }
            } catch (SecurityException ex) {
                ReflectionUtils.handleReflectionException(ex);
            } catch (NoSuchMethodException ex) {
                throw new IllegalStateException(String.format(
                        "@KafkaListener method '%s' found on bean target class '%s', " +
                                "but not found in any interface(s) for bean JDK proxy. Either " +
                                "pull the method up to an interface or switch to subclass (CGLIB) " +
                                "proxies by setting proxy-target-class/proxyTargetClass " +
                                "attribute to 'true'", method.getName(), method.getDeclaringClass().getSimpleName()), ex);
            }
        }
        return method;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

}
