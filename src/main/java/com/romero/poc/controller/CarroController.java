package com.romero.poc.controller;

import com.romero.poc.controller.request.CarroRequest;
import com.romero.poc.service.CarroService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@RequiredArgsConstructor
@Slf4j
@Controller
public class CarroController {


    private final CarroService carroService;

    @PostMapping("/carros")
    public ResponseEntity criarCarro(@RequestBody CarroRequest request){

        log.info("Criando carro");
        log.info(request.toString());
        carroService.salvar(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(request);
   }

   @GetMapping("/carros")
    public ResponseEntity searchCarros(){
      var carros=  carroService.searchAll();

        return ResponseEntity.ok(carros);
   }

   @GetMapping("/carros/{placa}")
    public ResponseEntity getCarro(@PathVariable String placa){
        var carro = carroService.findByPlaca(placa);
       return ResponseEntity.ok(carro);
   }
}
