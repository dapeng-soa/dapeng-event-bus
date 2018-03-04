package com.today.eventbus.spring;

import com.today.eventbus.ConsumerEndpoint;
import com.today.eventbus.annotation.KafkaListener;
import com.today.eventbus.utils.Contans;
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
        Optional<KafkaListener> classLevelListener = findListenerAnnotations(targetClass);
        //类上是否有注解
        final boolean hasClassListeners = classLevelListener.isPresent();
        //方法列表
        final List<Method> multiMethods = new ArrayList<>();
        Map<Method, Set<KafkaListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<Set<KafkaListener>>) method -> {
                    Set<KafkaListener> listenerMethods = findListenerAnnotations(method);
                    return (!listenerMethods.isEmpty() ? listenerMethods : null);
                });

        /**
         * 如果类上存在注解，该类下面必须有方法也有该注解注释
         */
        if (hasClassListeners && annotatedMethods.isEmpty()) {
            throw new IllegalArgumentException("@KafkaListener found on class type , " +
                    "but no @KafkaListener found on the method ,pleanse set it on the method");
        }

        if (annotatedMethods.isEmpty()) {
            this.logger.info("No @KafkaListener annotations found on bean type: " + bean.getClass());
        } else {
            // Non-empty set of methods
            for (Map.Entry<Method, Set<KafkaListener>> entry : annotatedMethods.entrySet()) {
                Method method = entry.getKey();
                for (KafkaListener listener : entry.getValue()) {
                    KafkaListenerInfo listenerMethodInfo = createListenerInfo(listener);
                    if (hasClassListeners) {
                        KafkaListenerInfo listenerClassinfo = createListenerInfo(classLevelListener.get());
                        processKafkaListener(listenerClassinfo, listenerMethodInfo, method, bean, beanName);
                    } else {
                        processKafkaListener(null, listenerMethodInfo, method, bean, beanName);
                    }
                }
            }

            if (this.logger.isDebugEnabled()) {
                this.logger.debug(annotatedMethods.size() + " @KafkaListener methods processed on bean '"
                        + beanName + "': " + annotatedMethods);
            }
        }
        return bean;
    }


    /**
     * 扫描bean 类上 是否有注解 @KafkaListener
     */
    private Optional<KafkaListener> findListenerAnnotations(Class<?> clazz) {
        KafkaListener ann = AnnotationUtils.findAnnotation(clazz, KafkaListener.class);
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
     * 处理注解信息
     *
     * @param classinfo
     * @param methodInfo
     * @param method
     * @param bean
     * @param beanName
     */
    protected void processKafkaListener(KafkaListenerInfo classinfo, KafkaListenerInfo methodInfo, Method method, Object bean, String beanName) {
        Method methodToUse = checkProxy(method, bean);
        ConsumerEndpoint endpoint = new ConsumerEndpoint();
        endpoint.setMethod(methodToUse);
        endpoint.setBean(bean);
        endpoint.setParameterTypes(Arrays.asList(method.getParameterTypes()));

        KafkaListenerInfo combineInfo = null;
        if (classinfo != null) {
            combineInfo = combine(classinfo, methodInfo);
        } else {
            combineInfo = methodInfo;
        }

        endpoint.setGroupId(combineInfo.getGroupId());
        endpoint.setTopic(combineInfo.getTopic());
        endpoint.setSerializer(combineInfo.getSerializer());
        endpoint.setKafkaHostKey(combineInfo.getKafkaHostKey());

        this.registrar.registerEndpoint(endpoint);
    }


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

    /**
     * 注解元信息 封装为 bean
     *
     * @param listener
     */
    private KafkaListenerInfo createListenerInfo(KafkaListener listener) {
        KafkaListenerInfo condition = new KafkaListenerInfo();
        condition.setTopic(listener.topic());
        condition.setGroupId(listener.groupId());
        condition.setSerializer(listener.serializer());
        condition.setKafkaHostKey(listener.kafkaHostKey());
        return condition;
    }


    private KafkaListenerInfo combine(KafkaListenerInfo classInfo, KafkaListenerInfo methodInfo) {
        KafkaListenerInfo combine = new KafkaListenerInfo();
        // topic
        if (!methodInfo.getTopic().isEmpty()) {
            combine.setTopic(methodInfo.getTopic());
        } else if (!classInfo.getTopic().isEmpty() && methodInfo.getTopic().isEmpty()) {
            combine.setTopic(classInfo.getTopic());
        }
        //groupId
        if (!methodInfo.getGroupId().isEmpty()) {
            combine.setGroupId(methodInfo.getGroupId());
        } else if (!classInfo.getGroupId().isEmpty() && methodInfo.getGroupId().isEmpty()) {
            combine.setGroupId(classInfo.getGroupId());
        }
        //kafkaHostKey
        if (methodInfo.getKafkaHostKey().equals(Contans.DEFAULT_CONSUMER_HOST_KEY)
                && !classInfo.getKafkaHostKey().equals(Contans.DEFAULT_CONSUMER_HOST_KEY)) {
            combine.setKafkaHostKey(classInfo.getKafkaHostKey());
        } else {
            combine.setKafkaHostKey(methodInfo.getKafkaHostKey());
        }
        //serializer
        // fixme 如果类上面包名固定，但是下面方法有另外的包名
        if (!classInfo.getSerializer().isEmpty()) {
            if (!methodInfo.getSerializer().isEmpty()) {
                String type = classInfo.getSerializer() + "." + methodInfo.getSerializer();
                combine.setSerializer(type);
            } else {
                combine.setSerializer(classInfo.getSerializer());
            }
        } else {
            combine.setSerializer(methodInfo.getSerializer());
        }
        return combine;
    }

}
