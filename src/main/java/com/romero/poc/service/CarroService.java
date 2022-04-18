package com.romero.poc.service;

import com.amazonaws.services.s3.AmazonS3;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.romero.poc.controller.request.CarroRequest;

import com.romero.poc.model.Carro;
import com.romero.poc.repository.CarroRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class CarroService {

    @Autowired
    private AmazonS3 clientS3;
    @Autowired
    CarroRepository carroRepository;
    @Value("${aws.s3.bucket.name}")
    private String bucketName;

    public void salvar(CarroRequest request) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(request);
            String fileName = request.getPlaca().concat(".json");

            clientS3.putObject(bucketName, fileName, json);
        }catch(JsonProcessingException e){
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public List<Carro> searchAll(){
        return carroRepository.searchAll();
    }

    @SneakyThrows
    public List<Carro> findByPlaca(String placa) {
        var s3Object = clientS3.getObject(bucketName, placa.concat(".json"));
        var json = new String(s3Object.getObjectContent().readAllBytes());
        ObjectMapper objectMapper = new ObjectMapper();
        var carro = objectMapper.readValue(json, Carro.class);
        return List.of(carro);
       // return carroRepository.findByPlaca(placa);
    }
}
