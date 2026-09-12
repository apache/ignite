# spring-vertx-web

#### 介绍
基于反射和vertx web构建的仿spring mvc框架，可以使用类似spring mvc方式使用注解简单开发基于vertx web的API接口，同时保留了vertx web的相关特性。
目前类支持@RestController,@RequestMapping,
####
接口支持@GetMapping,@PostMapping,@PutMapping,@DeleteMapping;
####
参数支持@RequestParameter和@RequestBody及对应的default值的定义。
####
项目可无缝集成至springboot中;
####
使用责任链模式编写保证拓展性；
####
使用策略模式加工厂模式确保代码优雅可读性强。
## 使用方法

引入该模块后，只需要在配置文件中使用:server.port配置端口：
```
server:
  port: 8787

```
在启动类中选择 WebApplicationType.NONE：
```
new SpringApplicationBuilder(demoApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
```
然后就可以在controller中使用mvc注解编写接口了：
```
@RestController
@RequestMapping("/demo2")
public class Demo2Controller {
    @GetMapping("/hello")
    public ResultDTO<String> hello(){
        return ResultDTO.success("hello world");
    }
}
```
启动后效果如下：
![img.png](img.png)
apifox测试结果：
![img_1.png](img_1.png)
#### 注意事项
所有的接口函数都会在vertx的NIO线程中执行，所以不要在这些方法中出现阻塞调用。

