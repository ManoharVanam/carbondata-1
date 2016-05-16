/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.testsuite.allqueries

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for all query on multiple datatypes
  * Manohar
  */
class AllDataTypesTestCase3 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    CarbonProperties.getInstance().addProperty("carbon.direct.surrogate","false")

    sql("create cube Carbon_automation_test dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )");
    sql("LOAD DATA FACT FROM '"+currentDirectory+"/src/test/resources/100_olap.csv' INTO Cube Carbon_automation_test partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')");
    sql("create cube myvmallTest dimensions(imei String,uuid String,MAC String,device_color String,device_shell_color String,device_name String,product_name String,ram String,rom String,cpu_clock String,series String,check_date String,check_month Integer ,check_day Integer,check_hour Integer,bom String,inside_name String,packing_date String,packing_year String,packing_month String,packing_day String,packing_hour String,customer_name String,deliveryAreaId String,deliveryCountry String,deliveryProvince String,deliveryCity String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time String,Active_check_year Integer,Active_check_month Integer,Active_check_day Integer,Active_check_hour Integer,ActiveAreaId String,ActiveCountry String,ActiveProvince String,Activecity String,ActiveDistrict String,Active_network String,Active_firmware_version String,Active_emui_version String,Active_os_version String,Latest_check_time String,Latest_check_year Integer,Latest_check_month Integer,Latest_check_day Integer,Latest_check_hour Integer,Latest_areaId String,Latest_country String,Latest_province String,Latest_city String,Latest_district String,Latest_firmware_version String,Latest_emui_version String,Latest_os_version String,Latest_network String,site String,site_desc String,product String,product_desc String) MEASURES(check_year Integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=3] )")
    sql("LOAD DATA fact from '" + currentDirectory + "/src/test/resources/100_VMALL_1_Day_DATA_2015-09-15.csv' INTO CUBE myvmallTest PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'imei,uuid,MAC,device_color,device_shell_color,device_name,product_name,ram,rom,cpu_clock,series,check_date,check_year,check_month,check_day,check_hour,bom,inside_name,packing_date,packing_year,packing_month,packing_day,packing_hour,customer_name,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,packing_list_no,order_no,Active_check_time,Active_check_year,Active_check_month,Active_check_day,Active_check_hour,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,Active_network,Active_firmware_version,Active_emui_version,Active_os_version,Latest_check_time,Latest_check_year,Latest_check_month,Latest_check_day,Latest_check_hour,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_firmware_version,Latest_emui_version,Latest_os_version,Latest_network,site,site_desc,product,product_desc')")
    sql("create cube IF NOT EXISTS traffic_2g_3g_4g dimensions(SOURCE_INFO String ,APP_CATEGORY_ID String ,APP_CATEGORY_NAME String ,APP_SUB_CATEGORY_ID String ,APP_SUB_CATEGORY_NAME String ,RAT_NAME String ,IMSI String ,OFFER_MSISDN String ,OFFER_ID String ,OFFER_OPTION_1 String ,OFFER_OPTION_2 String ,OFFER_OPTION_3 String ,MSISDN String ,PACKAGE_TYPE String ,PACKAGE_PRICE String ,TAG_IMSI String ,TAG_MSISDN String ,PROVINCE String ,CITY String ,AREA_CODE String ,TAC String ,IMEI String ,TERMINAL_TYPE String ,TERMINAL_BRAND String ,TERMINAL_MODEL String ,PRICE_LEVEL String ,NETWORK String ,SHIPPED_OS String ,WIFI String ,WIFI_HOTSPOT String ,GSM String ,WCDMA String ,TD_SCDMA String ,LTE_FDD String ,LTE_TDD String ,CDMA String ,SCREEN_SIZE String ,SCREEN_RESOLUTION String ,HOST_NAME String ,WEBSITE_NAME String ,OPERATOR String ,SRV_TYPE_NAME String ,TAG_HOST String ,CGI String ,CELL_NAME String ,COVERITY_TYPE1 String ,COVERITY_TYPE2 String ,COVERITY_TYPE3 String ,COVERITY_TYPE4 String ,COVERITY_TYPE5 String ,LATITUDE String ,LONGITUDE String ,AZIMUTH String ,TAG_CGI String ,APN String ,USER_AGENT String ,DAY String ,HOUR String ,`MIN` String ,IS_DEFAULT_BEAR integer ,EPS_BEARER_ID String ,QCI integer ,USER_FILTER String ,ANALYSIS_PERIOD String ) measures(UP_THROUGHPUT numeric,DOWN_THROUGHPUT numeric,UP_PKT_NUM numeric,DOWN_PKT_NUM numeric,APP_REQUEST_NUM numeric,PKT_NUM_LEN_1_64 numeric,PKT_NUM_LEN_64_128 numeric,PKT_NUM_LEN_128_256 numeric,PKT_NUM_LEN_256_512 numeric,PKT_NUM_LEN_512_768 numeric,PKT_NUM_LEN_768_1024 numeric,PKT_NUM_LEN_1024_ALL numeric,IP_FLOW_MARK numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (MSISDN) ,PARTITION_COUNT=3] )");
    sql("LOAD DATA fact from '"+ currentDirectory +"/src/test/resources/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO CUBE traffic_2g_3g_4g PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')");
  }
  override def afterAll {
    sql("drop cube Carbon_automation_test")
    sql("drop cube myvmallTest")
    sql("drop cube traffic_2g_3g_4g")

  }
  //TC_222
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY  LIKE Latest_areaId AND  Latest_DAY  LIKE Latest_HOUR") ({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY  LIKE Latest_areaId AND  Latest_DAY  LIKE Latest_HOUR"),
      Seq())
  })

  //TC_224
  test("select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from Carbon_automation_test) qq where babu NOT LIKE   Latest_MONTH") ({
    checkAnswer(
      sql("select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from Carbon_automation_test) qq where babu NOT LIKE   Latest_MONTH"),
      Seq())
  })

  //TC_262
  test("select count(imei) ,series from Carbon_automation_test group by series having sum (Latest_DAY) == 99") ({
    checkAnswer(
      sql("select count(imei) ,series from Carbon_automation_test group by series having sum (Latest_DAY) == 99"),
      Seq())
  })

  //TC_264
  test("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize = \"\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC") ({
    checkAnswer(
      sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize = \"\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"),
      Seq())
  })

  //TC_270
  test("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize < \"0RAM size\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC") ({
    checkAnswer(
      sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize < \"0RAM size\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"),
      Seq())
  })

  //TC_320
  test("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE gamePointId > 1.0E9 GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC") ({
    checkAnswer(
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE gamePointId > 1.0E9 GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"),
      Seq())
  })

  //TC_343
  test("SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation_test) SUB_QRY WHERE deliverycity IS NULL") ({
    checkAnswer(
      sql("SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation_test) SUB_QRY WHERE deliverycity IS NULL"),
      Seq())
  })

  //TC_409
  test("select  gamePointId from Carbon_automation_test where  modelId is  null") ({
    checkAnswer(
      sql("select  gamePointId from Carbon_automation_test where  modelId is  null"),
      Seq())
  })

  //TC_410
  test("select  contractNumber from Carbon_automation_test where bomCode is  null") ({
    checkAnswer(
      sql("select  contractNumber from Carbon_automation_test where bomCode is  null"),
      Seq())
  })

  //TC_411
  test("select  imei from Carbon_automation_test where AMSIZE is  null") ({
    checkAnswer(
      sql("select  imei from Carbon_automation_test where AMSIZE is  null"),
      Seq())
  })

  //TC_412
  test("select  bomCode from Carbon_automation_test where contractnumber is  null") ({
    checkAnswer(
      sql("select  bomCode from Carbon_automation_test where contractnumber is  null"),
      Seq())
  })

  //TC_413
  test("select  latest_day from Carbon_automation_test where  modelId is  null") ({
    checkAnswer(
      sql("select  latest_day from Carbon_automation_test where  modelId is  null"),
      Seq())
  })

  //TC_414
  test("select  latest_day from Carbon_automation_test where  deviceinformationid is  null") ({
    checkAnswer(
      sql("select  latest_day from Carbon_automation_test where  deviceinformationid is  null"),
      Seq())
  })

  //TC_415
  test("select  deviceinformationid from Carbon_automation_test where  modelId is  null") ({
    checkAnswer(
      sql("select  deviceinformationid from Carbon_automation_test where  modelId is  null"),
      Seq())
  })

  //TC_416
  test("select  deviceinformationid from Carbon_automation_test where  deviceinformationid is  null") ({
    checkAnswer(
      sql("select  deviceinformationid from Carbon_automation_test where  deviceinformationid is  null"),
      Seq())
  })

  //TC_417
  test("select  imei from Carbon_automation_test where  modelId is  null") ({
    checkAnswer(
      sql("select  imei from Carbon_automation_test where  modelId is  null"),
      Seq())
  })

  //TC_418
  test("select  imei from Carbon_automation_test where  deviceinformationid is  null") ({
    checkAnswer(
      sql("select  imei from Carbon_automation_test where  deviceinformationid is  null"),
      Seq())
  })

  //TC_112
  test("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test1") ({
    validateResult(
      sql("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test"),
      "TC_112.csv")
  })

  //TC_115
  test("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from Carbon_automation_test") ({
    validateResult(
      sql("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from Carbon_automation_test"),
      "TC_115.csv")
  })

  //TC_116
  test("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test") ({
    validateResult(
      sql("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test"),
      "TC_116.csv")
  })

  //TC_117
  test("select histogram_numeric(deviceInformationId,2)  as a from Carbon_automation_test") ({
    validateResult(
      sql("select histogram_numeric(deviceInformationId,2)  as a from Carbon_automation_test"),
      "TC_117.csv")
  })

  //TC_128
  test("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test") ({
    validateResult(
      sql("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test"),
      "TC_128.csv")
  })

  //TC_131
  test("select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from Carbon_automation_test") ({
    validateResult(
      sql("select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from Carbon_automation_test"),
      "TC_131.csv")
  })

  //TC_132
  test("select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test") ({
    validateResult(
      sql("select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test"),
      "TC_132.csv")
  })

  //TC_133
  test("select histogram_numeric(gamePointId,2)  as a from Carbon_automation_test") ({
    validateResult(
      sql("select histogram_numeric(gamePointId,2)  as a from Carbon_automation_test"),
      "TC_133.csv")
  })

  //TC_477
  test("select percentile(1,array(1)) from Carbon_automation_test") ({
    validateResult(
      sql("select percentile(1,array(1)) from Carbon_automation_test"),
      "TC_477.csv")
  })

  //TC_479
  test("select percentile(1,array('0.5')) from Carbon_automation_test") ({
    validateResult(
      sql("select percentile(1,array('0.5')) from Carbon_automation_test"),
      "TC_479.csv")
  })

  //TC_480
  test("select percentile(1,array('1')) from Carbon_automation_test") ({
    validateResult(
      sql("select percentile(1,array('1')) from Carbon_automation_test"),
      "TC_480.csv")
  })

  //TC_485
  test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test11") ({
    validateResult(
      sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),
      "TC_485.csv")
  })

  //TC_486
  test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test2") ({
    validateResult(
      sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),
      "TC_486.csv")
  })

  //TC_487
  test("select histogram_numeric(1, 5000)from Carbon_automation_test") ({
    validateResult(
      sql("select histogram_numeric(1, 5000)from Carbon_automation_test"),
      "TC_487.csv")
  })

  //TC_488
  test("select histogram_numeric(1, 1000)from Carbon_automation_test") ({
    validateResult(
      sql("select histogram_numeric(1, 1000)from Carbon_automation_test"),
      "TC_488.csv")
  })

  //TC_489
  test("select histogram_numeric(1, 500)from Carbon_automation_test1") ({
    validateResult(
      sql("select histogram_numeric(1, 500)from Carbon_automation_test"),
      "TC_489.csv")
  })

  //TC_490
  test("select histogram_numeric(1, 500)from Carbon_automation_test") ({
    validateResult(
      sql("select histogram_numeric(1, 500)from Carbon_automation_test"),
      "TC_490.csv")
  })

  //TC_491
  test("select collect_set(gamePointId) from Carbon_automation_test") ({
    validateResult(
      sql("select collect_set(gamePointId) from Carbon_automation_test"),
      "TC_491.csv")
  })

  //TC_492
  test("select collect_set(AMSIZE) from Carbon_automation_test1") ({
    validateResult(
      sql("select collect_set(AMSIZE) from Carbon_automation_test"),
      "TC_492.csv")
  })

  //TC_493
  test("select collect_set(bomcode) from Carbon_automation_test") ({
    validateResult(
      sql("select collect_set(bomcode) from Carbon_automation_test"),
      "TC_493.csv")
  })

  //TC_494
  test("select collect_set(imei) from Carbon_automation_test") ({
    validateResult(
      sql("select collect_set(imei) from Carbon_automation_test"),
      "TC_494.csv")
  })

  //TC_500
  test("select percentile(1,array('0.5')) from Carbon_automation_test1") ({
    validateResult(
      sql("select percentile(1,array('0.5')) from Carbon_automation_test"),
      "TC_500.csv")
  })

  //TC_501
  test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test1") ({
    validateResult(
      sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),
      "TC_501.csv")
  })

  //TC_502
  test("select collect_set(AMSIZE) from Carbon_automation_test") ({
    validateResult(
      sql("select collect_set(AMSIZE) from Carbon_automation_test"),
      "TC_502.csv")
  })

  //TC_513
  test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    checkAnswer(
      sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
      Seq())
  })

  //TC_563
  test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    checkAnswer(
      sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
      Seq())
  })

  //TC_612
  test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IN (\"4RAM size\",\"8RAM size\") GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    validateResult(sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IN (\"4RAM size\",\"8RAM size\") GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"), "TC_612.csv")
  })

  //TC_613
  test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    checkAnswer(
      sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
      Seq())
  })

  //TC_663
  test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    checkAnswer(
      sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
      Seq())
  })

  //TC_712
  test("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    checkAnswer(
      sql("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
      Seq())
  })

  //TC_760
  test("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    checkAnswer(
      sql("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
      Seq())
  })

  //TC_856
  test("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
    checkAnswer(
      sql("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
      Seq())
  })

  //VMALL_Per_TC_000
  test("select count(*) from    myvmallTest") ({
    checkAnswer(
      sql("select count(*) from    myvmallTest"),
      Seq(Row(1003)))
  })

  //VMALL_Per_TC_001
  test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY product_name ASC") ({
    validateResult(sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_001.csv")

  })

  //VMALL_Per_TC_002
  test("SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC") ({
    validateResult(sql("SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC"), "VMALL_Per_TC_002.csv")
  })

  //VMALL_Per_TC_003
  test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC") ({
    checkAnswer(
      sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC"),
      Seq(Row("Huawei4009",1)))
  })

  //VMALL_Per_TC_004
  test("SELECT device_color FROM (select * from myvmallTest) SUB_QRY GROUP BY device_color ORDER BY device_color ASC") ({
    validateResult(sql("SELECT device_color FROM (select * from myvmallTest) SUB_QRY GROUP BY device_color ORDER BY device_color ASC"), "VMALL_Per_TC_004.csv")
  })

  //VMALL_Per_TC_005
  test("SELECT product_name  FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC") ({
    validateResult(sql("SELECT product_name  FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC"), "VMALL_Per_TC_005.csv")
  })

  //VMALL_Per_TC_006
  test("SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC") ({
    validateResult(sql("SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_006.csv")
  })

  //VMALL_Per_TC_007
  test("select count(distinct imei) DistinctCount_imei from myvmallTest") ({
    checkAnswer(
      sql("select count(distinct imei) DistinctCount_imei from myvmallTest"),
      Seq(Row(1001)))
  })

  //VMALL_Per_TC_008
  test("Select count(imei),deliveryCountry  from myvmallTest group by deliveryCountry order by deliveryCountry asc") ({
    validateResult(sql("Select count(imei),deliveryCountry  from myvmallTest group by deliveryCountry order by deliveryCountry asc"), "VMALL_Per_TC_008.csv")
  })

  //VMALL_Per_TC_009
  test("select (t1.hnor6emui/t2.totalc)*100 from (select count (Active_emui_version)  as hnor6emui from myvmallTest where Active_emui_version=\"EmotionUI_2.1\")t1,(select count(Active_emui_version) as totalc from myvmallTest)t2") ({
    checkAnswer(
      sql("select (t1.hnor6emui/t2.totalc)*100 from (select count (Active_emui_version)  as hnor6emui from myvmallTest where Active_emui_version=\"EmotionUI_2.1\")t1,(select count(Active_emui_version) as totalc from myvmallTest)t2"),
      Seq(Row(0.09970089730807577)))
  })

  //VMALL_Per_TC_010
  test("select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmallTest where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmallTest)t2") ({
    checkAnswer(
      sql("select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmallTest where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmallTest)t2"),
      Seq(Row(0.0)))
  })

  //VMALL_Per_TC_011
  test("select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmallTest) sub where mydates<1000") ({
    checkAnswer(
      sql("select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmallTest) sub where mydates<1000"),
      Seq(Row(1000)))
  })

  //VMALL_Per_TC_012
  test("SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC") ({
    validateResult(sql("SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC"), "VMALL_Per_TC_012.csv")
  })

  //VMALL_Per_TC_013
  test("select count(imei)  DistinctCount_imei from myvmallTest where (Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3.863972\") OR (Active_emui_version=\"EmotionUI_2.843\" and Latest_emui_version=\"EmotionUI_3.863843\")") ({
    checkAnswer(
      sql("select count(imei)  DistinctCount_imei from myvmallTest where (Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3.863972\") OR (Active_emui_version=\"EmotionUI_2.843\" and Latest_emui_version=\"EmotionUI_3.863843\")"),
      Seq(Row(2)))
  })

  //VMALL_Per_TC_014
  test("select count(imei) as imeicount from myvmallTest where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')") ({
    checkAnswer(
      sql("select count(imei) as imeicount from myvmallTest where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')"),
      Seq(Row(2)))
  })

  //VMALL_Per_TC_B015
  test("SELECT product, count(distinct imei) DistinctCount_imei FROM myvmallTest GROUP BY product ORDER BY product ASC") ({
    validateResult(sql("SELECT product, count(distinct imei) DistinctCount_imei FROM myvmallTest GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B015.csv")
  })

  //VMALL_Per_TC_B016
  test("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC") ({
    validateResult(sql("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC"), "VMALL_Per_TC_B016.csv")
  })

  //VMALL_Per_TC_B017
  test("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC") ({
    checkAnswer(
      sql("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"),
      Seq(Row("SmartPhone_3998",1)))
  })

  //VMALL_Per_TC_B018
  test("SELECT Active_emui_version FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC") ({
    validateResult(sql("SELECT Active_emui_version FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC"), "VMALL_Per_TC_B018.csv")
  })

  //VMALL_Per_TC_B019
  test("SELECT product FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC") ({
    validateResult(sql("SELECT product FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B019.csv")
  })

  //VMALL_Per_TC_B020
  test("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC") ({
    validateResult(sql("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B020.csv")
  })

  //duplicate
  /*//VMALL_Per_TC_015
  test("SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmallTest    GROUP BY product ORDER BY product ASC") ({
  checkAnswer(
  sql("SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmallTest    GROUP BY product ORDER BY product ASC"),
  Seq())
  })

  //VMALL_Per_TC_016
  test("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC") ({
  validateResult(sql("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC"), "VMALL_Per_TC_016.csv")
  })

  //VMALL_Per_TC_017
  test("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC") ({
  validateResult(sql("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_017.csv")
  })

  //VMALL_Per_TC_018
  test("SELECT Active_emui_version FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC") ({
  validateResult(sql("SELECT Active_emui_version FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC"), "VMALL_Per_TC_018.csv")
  })

  //VMALL_Per_TC_019
  test("SELECT product FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC") ({
  validateResult(sql("SELECT product FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_019.csv")
  })

  //VMALL_Per_TC_020
  test("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC") ({
  validateResult(sql("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_020.csv")
  })*/

  //VMALL_Per_TC_021
  test("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'") ({
    checkAnswer(
      sql("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'"),
      Seq(Row("imeiA009863011","Honor63011")))
  })

  //VMALL_Per_TC_022
  test("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'") ({
    checkAnswer(
      sql("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'"),
      Seq(Row("imeiA009863011","Honor63011"),Row("imeiA009863012","Honor63012")))
  })

  //VMALL_Per_TC_023
  test("SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmallTest   ) SUB_QRY where series LIKE 'series1%' group by series") ({
    validateResult(sql("SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmallTest   ) SUB_QRY where series LIKE 'series1%' group by series"), "VMALL_Per_TC_023.csv")
  })

  //VMALL_Per_TC_024
  test("select product_name, count(distinct imei)  as imei_number from     myvmallTest    where imei='imeiA009863017' group by product_name") ({
    checkAnswer(
      sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest    where imei='imeiA009863017' group by product_name"),
      Seq(Row("Huawei3017",1)))
  })

  //VMALL_Per_TC_025
  test("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc") ({
    checkAnswer(
      sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc"),
      Seq(Row("Huawei3017",1)))
  })

  //VMALL_Per_TC_026
  test("select deliveryCity, count(distinct imei)  as imei_number from     myvmallTest     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc") ({
    checkAnswer(
      sql("select deliveryCity, count(distinct imei)  as imei_number from     myvmallTest     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc"),
      Seq(Row("deliveryCity17",2)))
  })

  //VMALL_Per_TC_027
  test("select device_color, count(distinct imei)  as imei_number from     myvmallTest     where bom='51090576_63017' group by device_color order by imei_number desc") ({
    checkAnswer(
      sql("select device_color, count(distinct imei)  as imei_number from     myvmallTest     where bom='51090576_63017' group by device_color order by imei_number desc"),
      Seq(Row("black3017",1)))
  })

  //VMALL_Per_TC_028
  test("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where product_name='Huawei3017' group by product_name order by imei_number desc") ({
    checkAnswer(
      sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where product_name='Huawei3017' group by product_name order by imei_number desc"),
      Seq(Row("Huawei3017",1)))
  })

  //VMALL_Per_TC_029
  test("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryprovince='Province_17' group by product_name order by imei_number desc") ({
    checkAnswer(
      sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryprovince='Province_17' group by product_name order by imei_number desc"),
      Seq(Row("Huawei3017",1),Row("Huawei3517",1)))
  })

  //VMALL_Per_TC_030
  test("select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmallTest     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc") ({
    checkAnswer(
      sql("select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmallTest     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc"),
      Seq(Row("517_GB","cpu_clock517",1),Row("17_GB","cpu_clock17",1)))
  })

  //VMALL_Per_TC_031
  test("select uuid,mac,device_color,count(distinct imei) from    myvmallTest    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color") ({
    checkAnswer(
      sql("select uuid,mac,device_color,count(distinct imei) from    myvmallTest    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color"),
      Seq(Row("uuidA009863017","MAC09863017","black3017",1)))
  })

  //VMALL_Per_TC_032
  test("select device_color,count(distinct imei)as imei_number  from     myvmallTest   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc") ({
    checkAnswer(
      sql("select device_color,count(distinct imei)as imei_number  from     myvmallTest   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc"),
      Seq(Row("black3987",1)))
  })

  //VMALL_Per_TC_033
  test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc") ({
    checkAnswer(
      sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc"),
      Seq(Row("Huawei3993","black3993",1)))
  })

  //VMALL_Per_TC_034
  test("select device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc") ({
    checkAnswer(
      sql("select device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc"),
      Seq(Row("black3972",1)))
  })

  //VMALL_Per_TC_035
  test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc") ({
    checkAnswer(
      sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc"),
      Seq(Row("Huawei3972","black3972",1)))
  })

  //VMALL_Per_TC_036
  test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc") ({
    checkAnswer(
      sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc"),
      Seq(Row("Huawei3987","black3987",1)))
  })

  //VMALL_Per_TC_037
  test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc") ({
    checkAnswer(
      sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc"),
      Seq(Row("Huawei3987","black3987",1)))
  })

  //VMALL_Per_TC_038
  test("select Latest_network, count(distinct imei) as imei_number from  myvmallTest  group by Latest_network") ({
    validateResult(sql("select Latest_network, count(distinct imei) as imei_number from  myvmallTest  group by Latest_network"), "VMALL_Per_TC_038.csv")
  })

  //VMALL_Per_TC_039
  test("select device_name, count(distinct imei) as imei_number from  myvmallTest  group by device_name") ({
    validateResult(sql("select device_name, count(distinct imei) as imei_number from  myvmallTest  group by device_name"), "VMALL_Per_TC_039.csv")
  })


  //VMALL_Per_TC_040
  test("select product_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name") ({
    validateResult(sql("select product_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name"), "VMALL_Per_TC_040.csv")
  })

  //VMALL_Per_TC_041
  test("select deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity") ({
    validateResult(sql("select deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity"), "VMALL_Per_TC_041.csv")
  })

  //VMALL_Per_TC_042
  test("select device_name, deliverycity,count(distinct imei) as imei_number from  myvmallTest  group by device_name,deliverycity") ({
    validateResult(sql("select device_name, deliverycity,count(distinct imei) as imei_number from  myvmallTest  group by device_name,deliverycity"), "VMALL_Per_TC_042.csv")
  })

  //VMALL_Per_TC_043
  test("select product_name, device_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name,device_name") ({
    validateResult(sql("select product_name, device_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name,device_name"), "VMALL_Per_TC_043.csv")
  })

  //VMALL_Per_TC_044
  test("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name1") ({
    validateResult(sql("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name"), "VMALL_Per_TC_044.csv")
  })

  //VMALL_Per_TC_045
  test("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name") ({
    validateResult(sql("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name"), "VMALL_Per_TC_045.csv")
  })

  //VMALL_Per_TC_046
  test("select check_day,check_hour, count(distinct imei) as imei_number from  myvmallTest  group by check_day,check_hour") ({
    checkAnswer(
      sql("select check_day,check_hour, count(distinct imei) as imei_number from  myvmallTest  group by check_day,check_hour"),
      Seq(Row(15,-1,1000),Row(null,null,1)))
  })

  //VMALL_Per_TC_047
  test("select device_color,product_name, count(distinct imei) as imei_number from  myvmallTest  group by device_color,product_name order by product_name limit 1000") ({
    validateResult(sql("select device_color,product_name, count(distinct imei) as imei_number from  myvmallTest  group by device_color,product_name order by product_name limit 1000"), "VMALL_Per_TC_047.csv")
  })

  //VMALL_Per_TC_048
  test("select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmallTest  group by packing_hour,deliveryCity,device_color order by deliveryCity  limit 1000") ({
    validateResult(sql("select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmallTest  group by packing_hour,deliveryCity,device_color order by deliveryCity  limit 1000"), "VMALL_Per_TC_048.csv")
  })

  //VMALL_Per_TC_049
  test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC") ({
    validateResult(sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_049.csv")
  })

  //VMALL_Per_TC_050
  test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC") ({
    checkAnswer(
      sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"),
      Seq(Row("Huawei3987",1)))
  })

  //VMALL_Per_TC_051
  test("SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmallTest  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC") ({
    validateResult(sql("SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmallTest  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC"), "VMALL_Per_TC_051.csv")
  })

  //VMALL_Per_TC_052
  test("SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmallTest  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC") ({
    checkAnswer(
      sql("SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmallTest  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"),
      Seq(Row("Huawei3987",1)))
  })

  //VMALL_Per_TC_053
  test("SELECT product_name FROM  myvmallTest  SUB_QRY GROUP BY product_name ORDER BY product_name ASC") ({
    validateResult(sql("SELECT product_name FROM  myvmallTest  SUB_QRY GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_053.csv")
  })

  //VMALL_Per_TC_054
  test("SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC") ({
    validateResult(sql("SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_054.csv")
  })

  //SmartPCC_Perf_TC_001
  test("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='HTC' and APP_CATEGORY_NAME='Web_Browsing' group by MSISDN")({
    checkAnswer(
      sql("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='HTC' and APP_CATEGORY_NAME='Web_Browsing' group by MSISDN"),
      Seq(Row("8613649905753", 2381)))
  })

  //SmartPCC_Perf_TC_002
  test("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total < 1073741824 order by total desc")({
    checkAnswer(
      sql("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total < 1073741824 order by total desc"),
      Seq(Row("8613893462639", 2874640), Row("8613993676885", 73783), Row("8618394185970", 23865), Row("8618793100458", 15112), Row("8618794812876", 14411), Row("8615120474362", 6936), Row("8613893853351", 6486), Row("8618393012284", 5700), Row("8613993800024", 5044), Row("8618794965341", 4840), Row("8613993899110", 4364), Row("8613519003078", 2485), Row("8613649905753", 2381), Row("8613893600602", 2346), Row("8615117035070", 1310), Row("8618700943475", 1185), Row("8613919791668", 928), Row("8615209309657", 290), Row("8613993104233", 280)))
  })

  //SmartPCC_Perf_TC_003
  test("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc")({
    checkAnswer(
      sql("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc"),
      Seq(Row("8613893462639", 2874640), Row("8613993676885", 73783)))
  })

  //SmartPCC_Perf_TC_004
  test("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number, sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_CATEGORY_NAME")({
    checkAnswer(
      sql("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number, sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_CATEGORY_NAME"),
      Seq(Row("Network_Admin", 5, 12402), Row("Web_Browsing", 6, 2972886), Row("IM", 2, 29565), Row("Tunnelling", 1, 4364), Row("Game", 1, 2485), Row("", 4, 24684)))
  })

  //SmartPCC_Perf_TC_005
  test("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_CATEGORY_NAME='Web_Browsing' group by APP_CATEGORY_NAME order by msidn_number desc")({
    checkAnswer(
      sql("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_CATEGORY_NAME='Web_Browsing' group by APP_CATEGORY_NAME order by msidn_number desc"),
      Seq(Row("Web_Browsing", 6, 2972886)))
  })

  //SmartPCC_Perf_TC_006
  test("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME"),
      Seq(Row("QQ_Media", 1, 5700), Row("DNS", 5, 12402), Row("QQ_IM", 1, 23865), Row("HTTP", 4, 2896722), Row("XiaYiDao", 1, 2485), Row("HTTP_Browsing", 1, 2381), Row("HTTPS", 1, 73783), Row("", 4, 24684), Row("SSL", 1, 4364)))
  })

  //SmartPCC_Perf_TC_007
  test("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_SUB_CATEGORY_NAME='HTTP' and TERMINAL_BRAND='MARCONI' group by APP_SUB_CATEGORY_NAME order by msidn_number desc")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_SUB_CATEGORY_NAME='HTTP' and TERMINAL_BRAND='MARCONI' group by APP_SUB_CATEGORY_NAME order by msidn_number desc"),
      Seq(Row("HTTP", 1, 2874640)))
  })

  //SmartPCC_Perf_TC_008
  test("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_BRAND")({
    checkAnswer(
      sql("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_BRAND"),
      Seq(Row("OPPO", 1, 928), Row("HTC", 2, 2661), Row("美奇", 1, 2485), Row("NOKIA", 1, 14411), Row("MARCONI", 1, 2874640), Row("SUNUP", 1, 290), Row("TIANYU", 1, 23865), Row("LANBOXING", 1, 4364), Row("BBK", 1, 6936), Row("SECURE", 1, 1185), Row("MOTOROLA", 3, 80137), Row("DAXIAN", 1, 6486), Row("LENOVO", 1, 2346), Row("", 1, 4840), Row("山水", 1, 5700), Row("SANGFEI", 1, 15112)))
  })

  //SmartPCC_Perf_TC_009
  test("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='MARCONI' group by TERMINAL_BRAND order by msidn_number desc")({
    checkAnswer(
      sql("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='MARCONI' group by TERMINAL_BRAND order by msidn_number desc"),
      Seq(Row("MARCONI", 1, 2874640)))
  })

  //SmartPCC_Perf_TC_010
  test("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_TYPE"),
      Seq(Row(" ", 2, 2875825), Row("SMARTPHONE", 8, 123420), Row("", 1, 4840), Row("FEATURE PHONE", 8, 42301)))
  })

  //SmartPCC_Perf_TC_011
  test("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by TERMINAL_TYPE order by total desc")({
    checkAnswer(
      sql("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by TERMINAL_TYPE order by total desc"),
      Seq(Row(" ", 2, 2875825), Row("SMARTPHONE", 8, 123420), Row("FEATURE PHONE", 8, 42301), Row("", 1, 4840)))
  })

  //SmartPCC_Perf_TC_012
  test("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by CGI")({
    checkAnswer(
      sql("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by CGI"),
      Seq(Row("460003772902063", 1, 73783), Row("460003788311323", 1, 5700), Row("460003777109392", 1, 6486), Row("460003787211338", 1, 1310), Row("460003776440020", 1, 5044), Row("460003773401611", 1, 2381), Row("460003767804016", 1, 4840), Row("460003784806621", 1, 1185), Row("460003787360026", 1, 14411), Row("460003785041401", 1, 6936), Row("460003766033446", 1, 15112), Row("460003776906411", 1, 4364), Row("460003782800719", 1, 2874640), Row("460003764930757", 1, 928), Row("460003788410098", 1, 23865), Row("460003763202233", 1, 2485), Row("460003763606253", 1, 290), Row("460003788100762", 1, 280), Row("460003784118872", 1, 2346)))
  })

  //SmartPCC_Perf_TC_013
  test("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where CGI='460003772902063' group by CGI order by total desc")({
    checkAnswer(
      sql("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where CGI='460003772902063' group by CGI order by total desc"),
      Seq(Row("460003772902063", 1, 73783)))
  })

  //SmartPCC_Perf_TC_014
  test("select RAT_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME")({
    checkAnswer(
      sql("select RAT_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME"),
      Seq(Row("GERAN", 19, 3046386)))
  })

  //SmartPCC_Perf_TC_015
  test("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by DAY,HOUR")({
    checkAnswer(
      sql("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by DAY,HOUR"),
      Seq(Row("8-1", "23", 19, 3046386)))
  })

  //SmartPCC_Perf_TC_016
  test("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where hour between 20 and 24 group by DAY,HOUR order by total desc")({
    checkAnswer(
      sql("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where hour between 20 and 24 group by DAY,HOUR order by total desc"),
      Seq(Row("8-1", "23", 19, 3046386)))
  })

  //SmartPCC_Perf_TC_017
  test("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME"),
      Seq(Row("HTTPS", "Web_Browsing", 1, 73783), Row("QQ_IM", "IM", 1, 23865), Row("HTTP_Browsing", "Web_Browsing", 1, 2381), Row("XiaYiDao", "Game", 1, 2485), Row("", "", 4, 24684), Row("SSL", "Tunnelling", 1, 4364), Row("HTTP", "Web_Browsing", 4, 2896722), Row("QQ_Media", "IM", 1, 5700), Row("DNS", "Network_Admin", 5, 12402)))
  })

  //SmartPCC_Perf_TC_018
  test("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  APP_CATEGORY_NAME='Web_Browsing' group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME order by total desc")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  APP_CATEGORY_NAME='Web_Browsing' group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME order by total desc"),
      Seq(Row("HTTP", "Web_Browsing", 4, 2896722), Row("HTTPS", "Web_Browsing", 1, 73783), Row("HTTP_Browsing", "Web_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_019
  test("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,TERMINAL_BRAND")({
    checkAnswer(
      sql("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,TERMINAL_BRAND"),
      Seq(Row("SECURE", "HTTP", 1, 1185), Row("HTC", "HTTP_Browsing", 1, 2381), Row("TIANYU", "QQ_IM", 1, 23865), Row("DAXIAN", "HTTP", 1, 6486), Row("BBK", "", 1, 6936), Row("山水", "QQ_Media", 1, 5700), Row("LENOVO", "", 1, 2346), Row("LANBOXING", "SSL", 1, 4364), Row("MOTOROLA", "DNS", 2, 6354), Row("MOTOROLA", "HTTPS", 1, 73783), Row("SANGFEI", "", 1, 15112), Row("美奇", "XiaYiDao", 1, 2485), Row("NOKIA", "HTTP", 1, 14411), Row("", "DNS", 1, 4840), Row("MARCONI", "HTTP", 1, 2874640), Row("OPPO", "DNS", 1, 928), Row("HTC", "DNS", 1, 280), Row("SUNUP", "", 1, 290)))
  })

  //SmartPCC_Perf_TC_020
  test("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' and TERMINAL_BRAND='HTC' group by TERMINAL_BRAND,APP_SUB_CATEGORY_NAME order by total desc")({
    checkAnswer(
      sql("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' and TERMINAL_BRAND='HTC' group by TERMINAL_BRAND,APP_SUB_CATEGORY_NAME order by total desc"),
      Seq(Row("HTC", "HTTP_Browsing", 1, 2381), Row("HTC", "DNS", 1, 280)))
  })

  //SmartPCC_Perf_TC_021
  test("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,RAT_NAME")({
    checkAnswer(
      sql("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,RAT_NAME"),
      Seq(Row("GERAN", "QQ_Media", 1, 5700), Row("GERAN", "HTTP", 4, 2896722), Row("GERAN", "XiaYiDao", 1, 2485), Row("GERAN", "", 4, 24684), Row("GERAN", "QQ_IM", 1, 23865), Row("GERAN", "DNS", 5, 12402), Row("GERAN", "HTTPS", 1, 73783), Row("GERAN", "SSL", 1, 4364), Row("GERAN", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_022
  test("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' group by APP_SUB_CATEGORY_NAME,RAT_NAME order by total desc")({
    checkAnswer(
      sql("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' group by APP_SUB_CATEGORY_NAME,RAT_NAME order by total desc"),
      Seq(Row("GERAN", "HTTP", 4, 2896722), Row("GERAN", "HTTPS", 1, 73783), Row("GERAN", "", 4, 24684), Row("GERAN", "QQ_IM", 1, 23865), Row("GERAN", "DNS", 5, 12402), Row("GERAN", "QQ_Media", 1, 5700), Row("GERAN", "SSL", 1, 4364), Row("GERAN", "XiaYiDao", 1, 2485), Row("GERAN", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_023
  test("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE"),
      Seq(Row("SMARTPHONE", "", 1, 2346), Row("SMARTPHONE", "QQ_IM", 1, 23865), Row("FEATURE PHONE", "QQ_Media", 1, 5700), Row("SMARTPHONE", "DNS", 3, 6634), Row("FEATURE PHONE", "HTTP", 1, 6486), Row("SMARTPHONE", "HTTPS", 1, 73783), Row("FEATURE PHONE", "XiaYiDao", 1, 2485), Row("SMARTPHONE", "HTTP_Browsing", 1, 2381), Row(" ", "HTTP", 2, 2875825), Row("FEATURE PHONE", "", 3, 22338), Row("", "DNS", 1, 4840), Row("FEATURE PHONE", "DNS", 1, 928), Row("FEATURE PHONE", "SSL", 1, 4364), Row("SMARTPHONE", "HTTP", 1, 14411)))
  })

  //SmartPCC_Perf_TC_024
  test("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  CGI in('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE order by total desc")({
    checkAnswer(
      sql("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  CGI in('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE order by total desc"),
      Seq(Row("SMARTPHONE", "HTTPS", 1, 73783), Row("SMARTPHONE", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_025
  test("select HOUR,cgi,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT+down_THROUGHPUT) as total from  traffic_2g_3g_4g  group by HOUR,cgi,APP_SUB_CATEGORY_NAME")({
    checkAnswer(
      sql("select HOUR,cgi,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT+down_THROUGHPUT) as total from  traffic_2g_3g_4g  group by HOUR,cgi,APP_SUB_CATEGORY_NAME"),
      Seq(Row("23", "460003788311323", "QQ_Media", 1, 5700), Row("23", "460003763606253", "", 1, 290), Row("23", "460003784806621", "HTTP", 1, 1185), Row("23", "460003776440020", "DNS", 1, 5044), Row("23", "460003772902063", "HTTPS", 1, 73783), Row("23", "460003782800719", "HTTP", 1, 2874640), Row("23", "460003776906411", "SSL", 1, 4364), Row("23", "460003788410098", "QQ_IM", 1, 23865), Row("23", "460003766033446", "", 1, 15112), Row("23", "460003763202233", "XiaYiDao", 1, 2485), Row("23", "460003764930757", "DNS", 1, 928), Row("23", "460003787211338", "DNS", 1, 1310), Row("23", "460003767804016", "DNS", 1, 4840), Row("23", "460003773401611", "HTTP_Browsing", 1, 2381), Row("23", "460003784118872", "", 1, 2346), Row("23", "460003785041401", "", 1, 6936), Row("23", "460003777109392", "HTTP", 1, 6486), Row("23", "460003788100762", "DNS", 1, 280), Row("23", "460003787360026", "HTTP", 1, 14411)))
  })

  //SmartPCC_Perf_TC_026
  test("select RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE")({
    checkAnswer(
      sql("select RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE"),
      Seq(Row("GERAN", "SSL", "FEATURE PHONE", 1, 4364), Row("GERAN", "HTTP", "SMARTPHONE", 1, 14411), Row("GERAN", "", "SMARTPHONE", 1, 2346), Row("GERAN", "QQ_IM", "SMARTPHONE", 1, 23865), Row("GERAN", "QQ_Media", "FEATURE PHONE", 1, 5700), Row("GERAN", "DNS", "SMARTPHONE", 3, 6634), Row("GERAN", "HTTPS", "SMARTPHONE", 1, 73783), Row("GERAN", "HTTP", "FEATURE PHONE", 1, 6486), Row("GERAN", "XiaYiDao", "FEATURE PHONE", 1, 2485), Row("GERAN", "HTTP_Browsing", "SMARTPHONE", 1, 2381), Row("GERAN", "HTTP", " ", 2, 2875825), Row("GERAN", "", "FEATURE PHONE", 3, 22338), Row("GERAN", "DNS", "", 1, 4840), Row("GERAN", "DNS", "FEATURE PHONE", 1, 928)))
  })

  //SmartPCC_Perf_TC_027
  test("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN")({
    checkAnswer(
      sql("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN"),
      Seq(Row("8613993676885", "HTTPS", 1, 73783), Row("8618394185970", "QQ_IM", 1, 23865), Row("8613993800024", "DNS", 1, 5044), Row("8613893600602", "", 1, 2346), Row("8613919791668", "DNS", 1, 928), Row("8618793100458", "", 1, 15112), Row("8618794812876", "HTTP", 1, 14411), Row("8618700943475", "HTTP", 1, 1185), Row("8613993104233", "DNS", 1, 280), Row("8615120474362", "", 1, 6936), Row("8615209309657", "", 1, 290), Row("8613893462639", "HTTP", 1, 2874640), Row("8615117035070", "DNS", 1, 1310), Row("8613519003078", "XiaYiDao", 1, 2485), Row("8613893853351", "HTTP", 1, 6486), Row("8613649905753", "HTTP_Browsing", 1, 2381), Row("8618794965341", "DNS", 1, 4840), Row("8613993899110", "SSL", 1, 4364), Row("8618393012284", "QQ_Media", 1, 5700)))
  })

  //SmartPCC_Perf_TC_028
  test("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN")({
    checkAnswer(
      sql("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN"),
      Seq(Row("8613993104233", "DNS", 1, 280), Row("8613649905753", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_029
  test("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN order by total desc")({
    checkAnswer(
      sql("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN order by total desc"),
      Seq(Row("8613649905753", "HTTP_Browsing", 1, 2381), Row("8613993104233", "DNS", 1, 280)))
  })

  //SmartPCC_Perf_TC_030
  test("select t2.MSISDN,t1.RAT_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.RAT_NAME,t2.MSISDN")({
    checkAnswer(
      sql("select t2.MSISDN,t1.RAT_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.RAT_NAME,t2.MSISDN"),
      Seq(Row("8618794965341", "GERAN", 1, 4840), Row("8613993676885", "GERAN", 1, 73783), Row("8613893462639", "GERAN", 1, 2874640), Row("8613993800024", "GERAN", 1, 5044), Row("8618394185970", "GERAN", 1, 23865), Row("8613893853351", "GERAN", 1, 6486), Row("8613919791668", "GERAN", 1, 928), Row("8613993104233", "GERAN", 1, 280), Row("8613893600602", "GERAN", 1, 2346), Row("8618393012284", "GERAN", 1, 5700), Row("8613519003078", "GERAN", 1, 2485), Row("8618793100458", "GERAN", 1, 15112), Row("8615117035070", "GERAN", 1, 1310), Row("8615120474362", "GERAN", 1, 6936), Row("8613649905753", "GERAN", 1, 2381), Row("8615209309657", "GERAN", 1, 290), Row("8613993899110", "GERAN", 1, 4364), Row("8618794812876", "GERAN", 1, 14411), Row("8618700943475", "GERAN", 1, 1185)))
  })

  //SmartPCC_Perf_TC_031
  test("select level, sum(sumUPdown) as total, count(distinct MSISDN) as MSISDN_count from (select MSISDN, t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT as sumUPdown, if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>52428800, '>50M', if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>10485760,'50M~10M',if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>1048576, '10M~1M','<1m'))) as level from  traffic_2g_3g_4g  t1) t2 group by level")({
    checkAnswer(
      sql("select level, sum(sumUPdown) as total, count(distinct MSISDN) as MSISDN_count from (select MSISDN, t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT as sumUPdown, if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>52428800, '>50M', if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>10485760,'50M~10M',if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>1048576, '10M~1M','<1m'))) as level from  traffic_2g_3g_4g  t1) t2 group by level"),
      Seq(Row("<1m", 171746, 18), Row("10M~1M", 2874640, 1)))
  })

  //SmartPCC_Perf_TC_032
  test("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G where MSISDN='8613993800024' group by TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G where MSISDN='8613993800024' group by TERMINAL_TYPE"),
      Seq(Row("SMARTPHONE", 1, 5044)))
  })

  //SmartPCC_Perf_TC_033
  test("select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g where MSISDN='8613519003078' group by TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g where MSISDN='8613519003078' group by TERMINAL_TYPE"),
      Seq(Row("FEATURE PHONE", 2485)))
  })

  //SmartPCC_Perf_TC_034
  test("select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G where MSISDN='8613993104233'")({
    checkAnswer(
      sql("select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G where MSISDN='8613993104233'"),
      Seq(Row("SMARTPHONE", 134, 146)))
  })

  //SmartPCC_Perf_TC_035
  test("select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G where MSISDN='8618394185970' and APP_CATEGORY_ID='2'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G where MSISDN='8618394185970' and APP_CATEGORY_ID='2'"),
      Seq(Row("GN", "2", "GERAN", "SMARTPHONE", 13934, 9931)))
  })

  //SmartPCC_Perf_TC_036
  test("select SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,AREA_CODE,CITY,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8615209309657' and APP_CATEGORY_ID='-1'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,AREA_CODE,CITY,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8615209309657' and APP_CATEGORY_ID='-1'"),
      Seq(Row("GN", "-1", "", "0930", "临夏", 200, 90)))
  })

  //SmartPCC_Perf_TC_037
  test("select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'"),
      Seq(Row("GN", "-1")))
  })

  //SmartPCC_Perf_TC_038
  test("select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'"),
      Seq(Row("GN", "-1", "LENOVO", "LENOVO A60", 1662, 684)))
  })

  //SmartPCC_Perf_TC_039
  test("select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'"),
      Seq(Row("GN", "16", "460003776906411", "8-1", "23", "0", 1647, 2717)))
  })

}