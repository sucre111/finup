val voice_info2 = spark.read.parquet("/user/hive/warehouse/alg_ods.db/kg_mobile_detail_voice_2_1")
val voice_info = spark.read.parquet("/user/hive/warehouse/alg_ods_full_new_filter.db/kg_mobile_detail_voice_d")

val ploan_m5 = ploan.filter($"create_time".gt(lit("2017-05-01"))).filter($"create_time".lt(lit("2017-06-01"))).select("apply_no","create_time","pass_time","status","is_over_due","source","user_id","core_customer_id","core_request_id")
val ploan_m5_fk = ploan_m5.filter("status in (10,11,12)")

val user = spark.read.parquet("hdfs://192.168.176.62:8020/user/hive/warehouse/cif.db/cif_jiea_user")
//val lsDF = spark.read.parquet("hdfs://192.168.176.62:8020/user/hive/warehouse/cif.db/cif_policy_dw_loansummary_static").dropDuplicates().select("core_lend_request_id","due").toDF("ls_core_lend_request_id","ls_due")
//val m5_fk = ploan_m5_fk.join(lsDF,$"core_request_id" === $"ls_core_lend_request_id", "left").drop("ls_core_lend_request_id")
val user_phone =user.select("id","id_no","mobile")
val m5_fk_phone = ploan_m5_fk.join(user_phone, $"user_id"===$"id","left").dropDuplicates()
