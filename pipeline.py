from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9745e725-cbf3-4a64-a39c-c9c023d49754"),
    covid_pos_person=Input(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55")
)
def all_clean_covid_pos_person(covid_pos_person):

    """
    Creates columns: date_of_birth, age_at_covid  
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
    Creates column: gender
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
    Creates column: race_ethnicity
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
    Creates column: zip_code
    Standardizes the values in zip:
    1. removes leading and training blanks
    2. truncates to first five characters
    3. only keeps values with 5 digit characters 
    """
    cpp_zip_df = ( 
        cpp_race_df
            .withColumn("zip_code", F.trim(cpp_race_df.zip))
            .withColumn("zip_code", F.when(F.length(F.col('zip_code')) >=  5, F.col('zip_code').substr(1,5)))
            .withColumn("zip_code", F.when(F.col('zip_code').rlike("[0-9]{5}"), F.col('zip_code')))
    )

    # .drop('year_of_birth','month_of_birth','day_of_birth','new_year_of_birth','new_month_of_birth','new_day_of_birth')

    return cpp_zip_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e"),
    covid_pos_person=Input(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55")
)
def clean_covid_pos_person(covid_pos_person):

    """
    Creates columns: date_of_birth, age_at_covid  
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
    Creates column: gender
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
    Creates column: race_ethnicity
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
    Creates column: zip_code
    Standardizes the values in zip:
    1. removes leading and training blanks
    2. truncates to first five characters
    3. only keeps values with 5 digit characters 
    """
    cpp_zip_df = ( 
        cpp_race_df
            .withColumn("zip_code", F.trim(cpp_race_df.zip))
            .withColumn("zip_code", F.when(F.length(F.col('zip_code')) >=  5, F.col('zip_code').substr(1,5)))
            .withColumn("zip_code", F.when(F.col('zip_code').rlike("[0-9]{5}"), F.col('zip_code')))
    )

    # .drop('year_of_birth','month_of_birth','day_of_birth','new_year_of_birth','new_month_of_birth','new_day_of_birth')

    return cpp_zip_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.203392f0-b875-453c-88c5-77ca5223739e"),
    clean_covid_pos_person=Input(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    our_concept_sets=Input(rid="ri.foundry.main.dataset.f80a92e0-cdc4-48d9-b4b7-42e60d42d9e0")
)
"""
Author: Elliott Fisher
Date:   06-10-2022
Description:
This process copies the logic found in the Logic Liasion conditions_of_interest transform 
created by Andrea Zhou.  
"""
def comorbidities_add(clean_covid_pos_person, our_concept_sets, condition_occurrence, concept_set_members):

    #bring in only cohort patient ids
    person_id_df = clean_covid_pos_person.select('person_id')
    
    """ 
    Get all conditions for current set of Covid+ patients    
    where the condition_start_date is not null
    """
    conditions_df = (
        condition_occurrence 
            .select('person_id', 'condition_start_date', 'condition_concept_id') 
            .where(F.col('condition_start_date').isNotNull()) 
            .withColumnRenamed('condition_start_date','visit_date') 
            .withColumnRenamed('condition_concept_id','concept_id') 
            .join(person_id_df,'person_id','inner')
    )

    comorbidity_concept_names_df = (
        our_concept_sets
            .filter(our_concept_sets.domain.contains('condition_occurrence'))
            .filter(our_concept_sets.domain.contains('comorbidity'))
            .select('concept_set_name','indicator_prefix')
    )        

    return fusion_df    
    

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

    proportion_of_patients_to_use = 1.

    return ALL_COVID_POS_PATIENTS.sample(False, proportion_of_patients_to_use, 111)
    

