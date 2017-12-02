
val kg_apply_basic_info = spark.read.parquet("/user/hive/warehouse/alg_ods_full_new_filter.db/kg_apply_basic_info")
// kg_apply_basic_info.filter("df_source = 'nirvana' and appyStatus not in ('1','2','LEND_REJECTED','12')").select()
val nir_fk = spark.read.parquet("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/nirvana_fk_bef_m12_parq")
val user = spark.read.parquet("/user/hive/warehouse/alg_ods_full_new_filter.db/kg_user_info")
val user_phone = user.select("user_id", "id_no", "mobile")
val nir_fk_phone = nir_fk.join(user_phone, Seq("user_id","id_no"), "left").dropDuplicates().toDF("kg_src","kg_user_id","kg_id_no","kg_apply_no","kg_core_lend_request_id","kg_apply_status","kg_submit_time","kg_df_source","kg_total_period","kg_pass_time","kg_src_create_time","kg_src_update_time","kg_timeStamp","user_mobile")

val voice_info = spark.read.parquet("/user/hive/warehouse/alg_ods_full_new_filter.db/kg_mobile_detail_voice_d")
val vi_dD = voice_info.dropDuplicates(Seq("id_no", "mobile", "counterpart_phone", "start_time")).filter($"start_time".gt(lit("2017-01-01")))
val apply_opinfo = nir_fk_phone.join(vi_dD, $"kg_id_no" === $"id_no", "left").filter("kg_submit_time > start_time")

val apply_tel_info = apply_opinfo.drop("business_name", "city", "communication_fee", "communication_loc", "id_no", "operator", "package_deals", "province", "src_create_time", "src_update_time", "create_time", "cif_dt", "communication_way", "communication_duration", "crawler_no", "id","mobile","isurge")

val zjphone280 = spark.read.option("header", true).csv("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/zj_280w").dropDuplicates

val apply_join_zj = zjphone280.join(apply_tel_info, $"agent_mobile" === $"counterpart_phone", "left")

val double_pair = apply_join_zj.select("kg_apply_no", "user_mobile", "counterpart_phone", "communication_way_code").dropDuplicates().groupBy("kg_apply_no", "user_mobile", "counterpart_phone").count().filter("count > 1").drop("count")

val applyTelInfoDouble = double_pair.join(apply_join_zj, Seq("kg_apply_no", "user_mobile", "counterpart_phone"), "left")

val times = applyTelInfoDouble.groupBy("kg_apply_no", "user_mobile", "counterpart_phone").count()
