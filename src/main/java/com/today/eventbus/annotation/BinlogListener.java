package com.today.eventbus.annotation;

import java.lang.annotation.*;

/**
 * 描述:
 *
 * @author hz.lei
 * @since 2018年03月07日 上午1:18
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface BinlogListener {

}
