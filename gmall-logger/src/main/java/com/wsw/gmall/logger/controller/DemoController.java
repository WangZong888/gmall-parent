package com.wsw.gmall.logger.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: gmall-parent
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-19 22:57
 **/
@RestController
public class DemoController {

    @GetMapping("test")
    public String test() {
        return "success";
    }
}
