package com.romero.poc.repository;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class AthenaRepository<T> {


    private final AthenaClient athenaClient;
    @Value("${aws.athena.default-database}")
    private final String dataBase;
    @Value("${aws.athena.output-bucket}")
    private final String bucketOutput;

    @Autowired
    public AthenaRepository(AthenaClient athenaClient, @Value("${aws.athena.default-database}") String dataBase,
                            @Value("${aws.athena.output-bucket}") String bucketOutput){
        this.athenaClient =athenaClient;
        this.dataBase = dataBase;
        this.bucketOutput = bucketOutput;
    }

    @SneakyThrows
    public List<T> execute(String query, Class<T> pojoClass) {
        String queryExecutionId =
                submitQuery(query);
        waitForQueryToComplete(queryExecutionId);
        return processResultRows(
                athenaClient, queryExecutionId, pojoClass);
    }

    private  <T> List<T> processResultRows(
            AthenaClient athenaClient, String queryExecutionId, Class<T> pojoClass) {
        List<T> res = new ArrayList<>();
        try {
            var getQueryResultsRequest =
                    GetQueryResultsRequest.builder()
                            .queryExecutionId(queryExecutionId).build();

            var getQueryResultsResults =
                    athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

            for (GetQueryResultsResponse result : getQueryResultsResults) {
                List<Row> results = result.resultSet().rows();
                res = processRows(results, pojoClass);
            }
        } catch (AthenaException e) {
            log.error("Failed to process with reason: {}", e.getMessage());
        }
        return res;
    }


    private String submitQuery(String query){
        var queryExecutionContext = QueryExecutionContext.builder().database(dataBase).build();
        var resultConfiguration = ResultConfiguration.builder().outputLocation(bucketOutput).build();
        var startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration).build();

        var response =  athenaClient.startQueryExecution(startQueryExecutionRequest);
        return response.queryExecutionId();
    }

    private void waitForQueryToComplete(String queryExecutionId){
        var qetQueryExecutionRequest = GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build();
        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;

        do{
            try {
                getQueryExecutionResponse = athenaClient.getQueryExecution(qetQueryExecutionRequest);

                String queryState =
                        getQueryExecutionResponse.queryExecution().status().state().toString();

                if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                    throw new RuntimeException("Error message: " + getQueryExecutionResponse
                            .queryExecution().status().stateChangeReason());
                } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                    throw new RuntimeException("The Amazon Athena query was cancelled.");
                } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                    isQueryStillRunning = false;
                } else {
                    log.info("Aguardando a busca finalizar ...");
                    Thread.sleep(1000l);
                }
            }catch(InterruptedException e){
                throw new RuntimeException("Erro slepp thread", e);
            }
        }while(isQueryStillRunning);

    }


    @SneakyThrows
    private <T> List<T> processRows(List<Row> row, Class<T> pojoClass) {
        List<T> res = new ArrayList<>();
        PropertyDescriptor[] targetPds = BeanUtils.getPropertyDescriptors(pojoClass);
        ArrayList<String> columnInfo = new ArrayList<>();
        for (int i = 0; i < row.size(); i++) {
            if( i == 0 ){
                columnInfo = holdColumnInfo(row.get(i).data());
                continue;
            }
            List<Datum> allData = row.get(i).data();
            T obj = createGenericInstance(
                    pojoClass.getDeclaredConstructor().newInstance(),
                    pojoClass);
            for(int j = 0; j < allData.size(); j++){
                Datum data = allData.get(j);
                for (PropertyDescriptor targetPd : targetPds) {
                    Method writeMethod = targetPd.getWriteMethod();
                    String fieldName = targetPd.getName();

                    if(fieldName != null  &&
                            fieldName.equalsIgnoreCase(columnInfo.get(j))){
                        writeMethod.invoke(obj, data.varCharValue());
                        break;
                    }
                }
            }
            res.add(obj);
        }
        return res;
    }

    private ArrayList<String> holdColumnInfo(List<Datum> columns){
        ArrayList<String> colList = new ArrayList<>();
        for(Datum eachCol: columns){
            colList.add(eachCol.varCharValue());
        }
        return colList;
    }

    private <T> T createGenericInstance(Object o, Class<T> clazz) {
        try {
            return clazz.cast(o);
        } catch(ClassCastException e) {
            return null;
        }
    }
}
