package io.vertx.webmvc.controller;

import io.vertx.core.json.JsonObject;
import io.vertx.webmvc.annotation.AuthedUser;
import io.vertx.webmvc.common.ResultDTO;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/base")
public class BaseRestController {


    @GetMapping("/:id")
    public ResultDTO<JsonObject> get(@PathVariable("id") String id,@AuthedUser String author,String name) {
        JsonObject data = new JsonObject();
        data.put("msg","hello world");
        data.put("id",id);
        data.put("name",name);
        return ResultDTO.success(data);
    }

    @AuthedUser
    @GetMapping("")
    public ResultDTO<String> list() {
        return ResultDTO.success("list hello world");
    }

    @DeleteMapping("/:id")
    public ResultDTO<String> delete(@PathVariable("id") String id) {
        return ResultDTO.success("hello world delete " + id);
    }

    @PostMapping("")
    public ResultDTO<String> save(@RequestBody JsonObject data) {

        return ResultDTO.success("hello world get" + data);
    }

    @PutMapping({"/:id","/update/:id"})
    public ResultDTO<String> update(@PathVariable("id") String id,@RequestParam("value") String value) {
        return ResultDTO.success("hello world put" + id);
    }
}
