package io.vertx.webmvc.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.lang.annotation.Annotation;

/**
 * Spring ApplicationContext 工具类
 *
 * @author zbw
 */
@Component
public class SpringUtils implements ApplicationContextAware {

    private static ApplicationContext context;

    /**
     * 通过class获取bean
     *
     * @param clz
     * @param <T>
     * @return
     */
    public static <T> T getBean(Class<T> clz) {
        return context.getBean(clz);
    }

    /**
     * 通过name获取Bean
     *
     * @param beanName
     * @return
     */
    public static Object getBean(String beanName) {
        return context.getBean(beanName);
    }

    // 通过name,以及Clazz返回指定的Bean
    public static <T> T getBean(String name, Class<T> clazz) {
        return context.getBean(name, clazz);
    }

    public static String[] getBeanNamesForType(@Nullable Class<?> aClass) {
        return context.getBeanNamesForType(aClass);
    }

    public static <A extends Annotation> A findAnnotationOnBean(String s, Class<A> aClass) {
        return context.findAnnotationOnBean(s, aClass);
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        if (context == null) {
            context = ctx;
        }

    }
}

