from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55"),
    covid_pos_sample=Input(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    person_lds=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
def covid_pos_person(covid_pos_sample, location, manifest, person_lds):

    with_person_df = (
        covid_pos_sample.join(
            person_lds.select(  'person_id','year_of_birth','month_of_birth','day_of_birth',
                                'ethnicity_concept_name','race_concept_name','gender_concept_name',
                                'location_id','data_partner_id'),
            covid_pos_sample.person_id == person_lds.person_id,
            how = "left"
        ).drop(person_lds.person_id)  
    )
    

    with_location_df = (
        with_person_df.join(
            location.select('location_id','city','state','zip','county'),
            with_person_df.location_id == location.location_id,
            how = "left"    
        ).drop(location.location_id)
    )

    with_manifest_df = (
        with_location_df.join(
            manifest.select('data_partner_id','run_date','cdm_name','cdm_version','shift_date_yn','max_num_shift_days'),
            with_location_df.data_partner_id == manifest.data_partner_id,
            how = "left" 
        ).drop(manifest.data_partner_id)

    )

    return with_manifest_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51"),
    ALL_COVID_POS_PATIENTS=Input(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903")
)
def covid_pos_sample(ALL_COVID_POS_PATIENTS):

    proportion_of_patients_to_use = .001

    return ALL_COVID_POS_PATIENTS.sample(False, proportion_of_patients_to_use, 111)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8d296bb7-d13c-45c0-b98f-634d97ca13a7"),
    trans_covid_pos_person=Input(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e")
)
def test_zip_logic(trans_covid_pos_person):

    cpp_zip_df = ( 
        trans_covid_pos_person
            .withColumn("zip_code", F.trim(trans_covid_pos_person.zip))
            .withColumn("zip_code",
                F.when(F.length(F.col('zip_code')) >=  5, F.col('zip_code').substr(1,5))
                .when(F.col('zip_code').rlike("[a-zA-Z]*"), F.col('zip_code'))
                .otherwise(None)
            ).select('zip','zip_code')
    )
    return cpp_zip_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e"),
    covid_pos_person=Input(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55")
)
def trans_covid_pos_person(covid_pos_person):

    """
    Creates new columns: date_of_birth, age_at_covid  
    Note: Sets null values of the following to 1:
        - year_of_birth
        - month_of_birth
        - day_of_birth
    """
    with_dob_age_df = (
        covid_pos_person
            .withColumn("new_year_of_birth",  
                        F.when(covid_pos_person.year_of_birth.isNull(),1)
                        .otherwise(covid_pos_person.year_of_birth))
            .withColumn("new_month_of_birth", 
                        F.when(covid_pos_person.month_of_birth.isNull(),1)
                        .otherwise(covid_pos_person.month_of_birth))
            .withColumn("new_day_of_birth", 
                        F.when(covid_pos_person.day_of_birth.isNull(),1)
                        .otherwise(covid_pos_person.day_of_birth))
            .withColumn("date_of_birth", 
                        F.concat_ws("-", F.col("new_year_of_birth"), F.col("new_month_of_birth"), F.col("new_day_of_birth")))
            .withColumn("date_of_birth", 
                        F.to_date("date_of_birth", format=None))
            .withColumn("age_at_covid", 
                        F.floor(F.months_between("first_diagnosis_date", "date_of_birth", roundOff=False)/12))
                        
    ).drop('new_year_of_birth','new_month_of_birth','new_day_of_birth')
    

    """
    Creates the new column: gender
    Contains standardized values from gender_concept_name so that:
    - Uppercase all versions of "male" and "female" strings
    - Replace non-MALE and FEMALE values with UNKNOWN 
    """
    cpp_gender_df = (
        with_dob_age_df
            .withColumn("gender",  
                F.when(F.upper(with_dob_age_df.gender_concept_name) == "MALE", "MALE")
                .when(F.upper(with_dob_age_df.gender_concept_name) == "FEMALE", "FEMALE")
                .otherwise("UNKNOWN")
            )
    )

    """
    Creates the new column: race_ethnicity
    Contains standardized values from ethnicity_concept_name and race_concept_name

    In data, but currentally set to UNKNOWN
    Barbadian
    Dominica Islander
    Trinidadian
    West Indian
    Jamaican
    African
    Madagascar
    Maldivian
    """
    cpp_race_df = ( 
        cpp_gender_df
            .withColumn("race_ethnicity", 
                F.when(F.col("ethnicity_concept_name") == 'Hispanic or Latino', "Hispanic or Latino Any Race")
                .when(F.col("race_concept_name").contains('Hispanic'), "Hispanic or Latino Any Race")
                .when(F.col("race_concept_name").contains('Black'), "Black or African American Non-Hispanic")
                .when(F.col("race_concept_name") == ('African American'), "Black or African American Non-Hispanic")                
                .when(F.col("race_concept_name").contains('White'), "White Non-Hispanic")
                .when(F.col("race_concept_name") == "Asian or Pacific Islander", "Unknown") # why is this unknown?
                .when(F.col("race_concept_name").contains('Asian'), "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Filipino'), "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Chinese'), "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Korean'), "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Vietnamese'), "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Japanese'), "Asian Non-Hispanic")                  
                .when(F.col("race_concept_name").contains('Bangladeshi'), "Asian Non-Hispanic") #
                .when(F.col("race_concept_name").contains('Pakistani'), "Asian Non-Hispanic")   #
                .when(F.col("race_concept_name").contains('Nepalese'), "Asian Non-Hispanic")    #
                .when(F.col("race_concept_name").contains('Laotian'), "Asian Non-Hispanic")     #
                .when(F.col("race_concept_name").contains('Taiwanese'), "Asian Non-Hispanic")   #                                     
                .when(F.col("race_concept_name").contains('Thai'), "Asian Non-Hispanic")        #
                .when(F.col("race_concept_name").contains('Sri Lankan'), "Asian Non-Hispanic")  #    
                .when(F.col("race_concept_name").contains('Burmese'), "Asian Non-Hispanic")     # 
                .when(F.col("race_concept_name").contains('Okinawan'), "Asian Non-Hispanic")    #                                                           
                .when(F.col("race_concept_name").contains('Cambodian'), "Asian Non-Hispanic")   #
                .when(F.col("race_concept_name").contains('Bhutanese'), "Asian Non-Hispanic")   #
                .when(F.col("race_concept_name").contains('Singaporean'), "Asian Non-Hispanic") #
                .when(F.col("race_concept_name").contains('Hmong'), "Asian Non-Hispanic")       #
                .when(F.col("race_concept_name").contains('Malaysian'), "Asian Non-Hispanic")   # 
                .when(F.col("race_concept_name").contains('Indonesian'), "Asian Non-Hispanic")  #               
                .when(F.col("race_concept_name").contains('Pacific'), "Native Hawaiian or Other Pacific Islander Non-Hispanic")
                .when(F.col("race_concept_name").contains('Polynesian'), "Native Hawaiian or Other Pacific Islander Non-Hispanic")                      
                .when(F.col("race_concept_name").contains('Native Hawaiian'), "Native Hawaiian or Other Pacific Islander Non-Hispanic") #                 
                .when(F.col("race_concept_name").contains('Micronesian'), "Native Hawaiian or Other Pacific Islander Non-Hispanic")     #
                .when(F.col("race_concept_name").contains('Melanesian'), "Native Hawaiian or Other Pacific Islander Non-Hispanic")      # 
                .when(F.col("race_concept_name").contains('Other'), "Other Non-Hispanic")    #??
                .when(F.col("race_concept_name").contains('Multiple'), "Other Non-Hispanic") #?? 
                .when(F.col("race_concept_name").contains('More'), "Other Non-Hispanic")     #??  
                .otherwise("UNKNOWN")
            )
    )

    """
    monitor_df = (
        Air_quality_monitor_data_for_N3C_upload
            .select('aqs_site_id', Air_quality_monitor_data_for_N3C_upload.zcta.cast("string").alias("zcta"), 'Longitude', 'Latitude')
            .withColumn("zcta", F.when(F.length(F.col("zcta")) >  5, F.substring('zcta', 1,5)).otherwise(F.col("zcta")))
            .filter(F.col("zcta").isNotNull())
            .filter(F.col("zcta").rlike('[0-9]{5}'))
            .distinct()
    ) 
    print('monitor_df row count', monitor_df.count(), sep=': ')
    
    zip_to_zcta_df = (
        ZIP_to_ZCTA_Crosswalk_2021
            .withColumn("ZCTA", F.when(F.length(F.col("ZCTA")) >  5, F.substring('ZCTA', 1,5)).otherwise(F.col("ZCTA")))
            .withColumn("ZIP_CODE", F.when(F.length(F.col("ZIP_CODE")) >  5, F.substring('ZIP_CODE', 1,5)).otherwise(F.col("ZIP_CODE")))
            .withColumn("ZIP_CODE", F.when(F.length(F.col("ZIP_CODE")) <  5, F.lpad('ZIP_CODE', 5, '0')).otherwise(F.col("ZIP_CODE")))
            .withColumn("zcta_matches_zip", F.when(F.col("ZIP_CODE") == F.col("ZCTA"), F.lit('1') ).otherwise(F.lit('0')))

    """

    # .drop('year_of_birth','month_of_birth','day_of_birth','new_year_of_birth','new_month_of_birth','new_day_of_birth')

    return cpp_race_df

