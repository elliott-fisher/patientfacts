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
    ALL_COVID_POS_PERSONS=Input(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903")
)
def covid_pos_sample(ALL_COVID_POS_PERSONS):

    proportion_of_patients_to_use = .001

    return ALL_COVID_POS_PERSONS.sample(False, proportion_of_patients_to_use, 111)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e"),
    covid_pos_person=Input(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55")
)
def trans_covid_pos_person(covid_pos_person):

    """
    Calculates date of birth and age at Covid+
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
            .withColumn("date_of_birth", F.concat_ws("-", F.col("new_year_of_birth"), F.col("new_month_of_birth"), F.col("new_day_of_birth")))
            .withColumn("date_of_birth", F.to_date("date_of_birth", format=None))
            .withColumn("age_at_covid", F.floor(F.months_between("first_diagnosis_date", "date_of_birth", roundOff=False)/12))
    ).drop('year_of_birth','month_of_birth','day_of_birth','new_year_of_birth','new_month_of_birth','new_day_of_birth')
    

    # Gender
    cpp_gender_df = (
        with_dob_age_df
            .withColumn("gender_concept_name2",   F.when(F.upper(with_dob_age_df.gender_concept_name) == "MALE", "MALE")
                                                .F.when(F.upper(with_dob_age_df.gender_concept_name) == "FEMALE", "FEMALE")
                                                .otherwise("UNKNOWN")
            )
    )

    # Ethncity

    return cpp_gender_df

