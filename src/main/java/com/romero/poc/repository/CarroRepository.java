package com.romero.poc.repository;


import com.romero.poc.model.Carro;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;



import java.util.List;

@RequiredArgsConstructor
@Slf4j
@Component
public class CarroRepository {

    private final AthenaRepository<Carro> athenaRepository;
    private static final String SEARCH_ALL = "select * from table_carros as c";
    private static final String FIND_BY_PLACA = "select * from table_carros as c where upper(c.placa) = upper(':placa')";

    public List<Carro> searchAll(){
        return athenaRepository.execute(SEARCH_ALL, Carro.class);
    }

    public List<Carro> findByPlaca(String placa) {
      var query = FIND_BY_PLACA.replaceAll(":placa", placa);
        return athenaRepository.execute(query, Carro.class);
    }
}
