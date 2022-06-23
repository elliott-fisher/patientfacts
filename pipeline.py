from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810"),
    pf_visits=Input(rid="ri.foundry.main.dataset.c4d2279d-88e2-4360-90f2-43df60f1961f")
)
"""
================================================================================
Final node

================================================================================
"""
def COVID_POS_PERSON_FACT(pf_visits):

    pf_df = pf_visits

    return pf_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ce1538cf-712c-492a-8f8c-adb5afa71b08"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
def explore_m_to_m(microvisit_to_macrovisit_lds):
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e"),
    pf_locations=Input(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55")
)
def pf_clean(pf_locations):

    pf_df = pf_locations

    """
    Creates columns: date_of_birth, age_at_covid  
    Note: Sets null values of the following to 1:
        - year_of_birth
        - month_of_birth
        - day_of_birth
    """
    with_dob_age_df = (
        pf_df
            .withColumn("new_year_of_birth",  
                        F.when(pf_df.year_of_birth.isNull(),1)
                        .otherwise(pf_df.year_of_birth))
            .withColumn("new_month_of_birth", 
                        F.when(pf_df.month_of_birth.isNull(),1)
                        .otherwise(pf_df.month_of_birth))
            .withColumn("new_day_of_birth", 
                        F.when(pf_df.day_of_birth.isNull(),1)
                        .otherwise(pf_df.day_of_birth))
            .withColumn("date_of_birth", 
                        F.concat_ws("-", F.col("new_year_of_birth"), F.col("new_month_of_birth"), F.col("new_day_of_birth")))
            .withColumn("date_of_birth", 
                        F.to_date("date_of_birth", format=None))
            .withColumn("age_at_covid", 
                        F.floor(F.months_between("first_poslab_or_diagnosis_date", "date_of_birth", roundOff=False)/12))
                        
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

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f561b69a-b3e6-492e-a54e-88c5b4ae0b7e"),
    pf_clean=Input(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e"),
    visit_comobidities=Input(rid="ri.foundry.main.dataset.203392f0-b875-453c-88c5-77ca5223739e")
)
def pf_comorbidities(visit_comobidities, pf_clean):

    comorbidity_by_visits   = visit_comobidities
    pf_df                   = pf_clean

    df = comorbidity_by_visits.drop('comorbidity_start_date')

    # compress comorbidities flags into one record per patient
    comorbidity_by_patient_df = (
        df
            .groupBy('person_id')
            .agg(*[F.max(col).alias(col) for col in df.drop('person_id', 'null').columns]) 
    )

    # add in person_id values for patients w/o comorbidities; fill nulls with 0
    all_patients = (
        pf_df
            .select('person_id')
            .join(comorbidity_by_patient_df, 'person_id', 'left')
            .na.fill(0)
    )

    # add in all non-comorbidity patient facts
    all_patients_data = pf_df.join(all_patients, 'person_id', 'left')

    return all_patients_data
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    person_lds=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66"),
    pf_sample=Input(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51")
)
"""
Drops out antigen only records
Adds in all person columns
Gets person address info from location
Gets treating institutions from manifest 
"""
def pf_locations(pf_sample, location, manifest, person_lds):

    pf_df = pf_sample

    # Drop rows with antibody only diagnosis 
    covid_pos_no_antibody_diag_df = pf_df.filter(F.col('first_poslab_or_diagnosis_date').isNotNull())

    with_person_df = (
        covid_pos_no_antibody_diag_df
            .join(
                person_lds.select('person_id','year_of_birth','month_of_birth','day_of_birth',
                                  'ethnicity_concept_name','race_concept_name','gender_concept_name',
                                  'location_id','data_partner_id'),
                covid_pos_no_antibody_diag_df.person_id == person_lds.person_id,
                how = "left"
        ).drop(person_lds.person_id)  
    ).drop(covid_pos_no_antibody_diag_df.first_antigen_or_poslab_or_diagnosis_date)
    

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
def pf_sample(ALL_COVID_POS_PATIENTS):

    proportion_of_patients_to_use = 1.

    return ALL_COVID_POS_PATIENTS.sample(False, proportion_of_patients_to_use, 111)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c4d2279d-88e2-4360-90f2-43df60f1961f"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    our_concept_sets=Input(rid="ri.foundry.main.dataset.f80a92e0-cdc4-48d9-b4b7-42e60d42d9e0"),
    pf_comorbidities=Input(rid="ri.foundry.main.dataset.f561b69a-b3e6-492e-a54e-88c5b4ae0b7e")
)
def pf_visits( microvisit_to_macrovisit_lds, our_concept_sets, concept_set_members, pf_comorbidities):

    pf_df = pf_comorbidities

    # Reduce patient columns and create column with the number of 
    # of days between the poslab and diagnosis dates
    pf1_df = (
        pf_df
            .select('person_id', 'first_pos_pcr_antigen_date', 'first_pos_diagnosis_date', 'first_poslab_or_diagnosis_date')
            .withColumn('poslab_minus_diag_date', F.datediff('first_pos_pcr_antigen_date', 'first_pos_diagnosis_date'))
    )

    # Reduce microvisit_to_macrovisit_lds columns and joined to contain patients
    pf_visits_df = (
        microvisit_to_macrovisit_lds
            .select('person_id','visit_start_date','visit_concept_id','macrovisit_start_date','macrovisit_end_date')
            .join(pf1_df,'person_id','inner')  
    )

    """
    ================================================================================
    Potential Parameters 
    --------------------
    requires_lab_and_diagnosis
    - Use for switching between using either:
        - first poslab date AND diag date (= True)
        - first poslab date OR diag date  (= False)

    num_days_before / num_days_after
     - these values need to be discussed!!!  
    ================================================================================ 
    """
    requires_lab_and_diagnosis = False
    num_days_before = 1
    num_days_after = 16

    """
    ================================================================================
    Get list of Emergency Dept Visit concept_set_name values from our spreadsheet 
    and use to create a list of associated concept_id values  
    ================================================================================
    """
    er_concept_names = list(
        our_concept_sets
            .filter(our_concept_sets.ed_visit == 1)
            .select('concept_set_name').toPandas()['concept_set_name']
    )    
    er_concept_ids = (
        list(concept_set_members
                .where(( concept_set_members.concept_set_name.isin(er_concept_names)) & 
                        (concept_set_members.is_most_recent_version == 'true'))
                .select('concept_id').toPandas()['concept_id']
        )
    )
    print(er_concept_ids)

    """
    ================================================================================ 
    Get Emergency Dept visits (null macrovisit_start_date values) and
    create the following columns: 
    poslab_minus_ER_date      - used for hospitalizations that require *both*
                                poslab and diagnosis
    first_index_minus_ER_date - used for hospitalizations that require *either*
                                poslab or diagnosis
    ================================================================================                                
    """
    ER_df = (
        pf_visits_df
            .where(pf_visits_df.macrovisit_start_date.isNull() & (pf_visits_df.visit_concept_id.isin(er_concept_ids)))
            .withColumn('poslab_minus_ER_date', 
                F.datediff('first_pos_pcr_antigen_date',     'visit_start_date'))
            .withColumn("first_index_minus_ER_date", 
                F.datediff("first_poslab_or_diagnosis_date", "visit_start_date"))         
    )

    """
    ================================================================================

    GET ED VISITS    

    ================================================================================
    """
    if requires_lab_and_diagnosis:
        ER_df = (
            ER_df
                .withColumn("poslab_associated_ER", 
                    F.when(F.col('poslab_minus_ER_date').between(-num_days_after, num_days_before), 1).otherwise(0))
                .withColumn("poslab_and_diag_associated_ER", 
                    F.when( (F.col('poslab_associated_ER') == 1) & 
                            (F.col('poslab_minus_diag_date').between(-num_days_after, num_days_before)), 1).otherwise(0))
                .where(F.col('poslab_and_diag_associated_ER') == 1)
                .withColumnRenamed('visit_start_date', 'covid_ER_only_start_date')
                .select('person_id', 'covid_ER_only_start_date')
                .dropDuplicates()
        )     
    else:
        ER_df = (
            ER_df
                .withColumn("poslab_or_diag_associated_ER", 
                    F.when(F.col('first_index_minus_ER_date').between(-num_days_after, num_days_before), 1).otherwise(0))
                .where(F.col('poslab_or_diag_associated_ER') == 1)
                .withColumnRenamed('visit_start_date', 'covid_ER_only_start_date')
                .select('person_id', 'covid_ER_only_start_date')
                .dropDuplicates()
        )        

    """ 
    ================================================================================    
    Get Hospitalization visits (non-null macrovisit_start_date values) and
    create the following columns: 
    poslab_minus_hosp_date      - used for hospitalizations that require *both*
                                  poslab and diagnosis
    first_index_minus_hosp_date - used for hospitalizations that require *either*
                                  poslab or diagnosis
    ================================================================================                                  
    """
    df_hosp = (
        pf_visits_df
            .where(pf_visits_df.macrovisit_start_date.isNotNull())
            .withColumn("poslab_minus_hosp_date", 
                F.datediff("first_pos_pcr_antigen_date",     "macrovisit_start_date"))
            .withColumn("first_index_minus_hosp_date", 
                F.datediff("first_poslab_or_diagnosis_date", "macrovisit_start_date"))    
    )

    """
    ================================================================================
    To have a hospitalization associated with *both* Positive PCR/Antigen test  
    and Covid Diagnosis, the test and diagnosis date need to be close together
    and the test and hospitalization must be close together. 
    
    Specifically:
    1. The hosp date must be within [num_days_before, num_days_after] of the poslab date 
       AND
    2. The diag date must be within [num_days_before, num_days_after] of the poslab date

    Example:
    ==============================================================
    [num_days_before, num_days_after] = [1,16]
    poslab date     = June 10 [June 9, June 16]
    diag date       = June 12 
    hosp date       = June 22
    
    1. Hospitalization must occur between June 9 and June 26: true
    2. Diagnosis date  must occur between June 9 and June 26: true
    ==============================================================

    Otherwise, to get hospitalizations associated with *either* a positive 
    PCR/Antigen test *or* a positive Covid Diagnosis, the first index date
    (whichever comes first:  PCR/Antigen or Diagnosis date) and the 
    hospitalization date must be close together. 
    ================================================================================    
    """
    if requires_lab_and_diagnosis:
        df_hosp = (
            df_hosp
                .withColumn("poslab_associated_hosp", 
                    F.when(F.col('poslab_minus_hosp_date').between(-num_days_after, num_days_before), 1).otherwise(0))
                .withColumn("poslab_and_diag_associated_hosp", 
                    F.when( (F.col('poslab_associated_hosp') == 1) & 
                            (F.col('poslab_minus_diag_date').between(-num_days_after, num_days_before)), 1).otherwise(0))
                .where(F.col('poslab_and_diag_associated_hosp') == 1)
                .withColumnRenamed('macrovisit_start_date', 'covid_hospitalization_start_date')
                .withColumnRenamed('macrovisit_end_date',   'covid_hospitalization_end_date')
                .select('person_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date')
                .dropDuplicates()
        )     
    else:
        df_hosp = (
            df_hosp
                .withColumn("poslab_or_diag_associated_hosp", 
                    F.when(F.col('first_index_minus_hosp_date').between(-num_days_after, num_days_before), 1).otherwise(0))
                .where(F.col('poslab_or_diag_associated_hosp') == 1)
                .withColumnRenamed('macrovisit_start_date', 'covid_hospitalization_start_date')
                .withColumnRenamed('macrovisit_end_date',   'covid_hospitalization_end_date')
                .select('person_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date')
                .dropDuplicates()
        )
    

    # Join er and hosp dataframes
    er_hosp_df = df_hosp.join(ER_df,'person_id', 'outer')

    """
    ================================================================================
    Collapse all values to one row per person using min start and end dates.

    DISCUSS:
    ??? Why is this the minimum End Date ???? 
    ================================================================================    
    """
    er_hosp_agg_df = (
        er_hosp_df
        .groupby('person_id')
        .agg(F.min('covid_ER_only_start_date').alias('first_covid_ER_only_start_date'),
             F.min('covid_hospitalization_start_date').alias('first_COVID_hospitalization_start_date'),
             F.min('covid_hospitalization_end_date').alias('first_COVID_hospitalization_end_date')
        )
    )

    # Join in all person facts
    pf_er_hosp_agg_df = pf_df.join(er_hosp_agg_df, 'person_id', 'left')    

    return pf_er_hosp_agg_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.203392f0-b875-453c-88c5-77ca5223739e"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    our_concept_sets=Input(rid="ri.foundry.main.dataset.f80a92e0-cdc4-48d9-b4b7-42e60d42d9e0"),
    pf_clean=Input(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e")
)
"""
Author: Elliott Fisher
Date:   06-10-2022
Description:
This process copies the logic found in the Logic Liasion conditions_of_interest transform 
created by Andrea Zhou.  

Notes:
- Comorbidities (from condition_occurrence) with null condition_start_date values
  are dropped
- Comorbidities are included if recorded at anytime (i.e. could be after Covid+) 

"""
def visit_comobidities(pf_clean, our_concept_sets, condition_occurrence, concept_set_members):
    
    pf_df = pf_clean
    

    # Get comorbidity concept_set_name values from our list 
    comorbidity_concept_names_df = (
        our_concept_sets
            .filter(our_concept_sets.domain.contains('condition_occurrence'))
            .filter(our_concept_sets.comorbidity == 1)
            .select('concept_set_name','column_name')
    )

    # Get most recent version of comorbidity concept_id values from concept_set_members 
    comorbidity_concept_set_members_df = (
        concept_set_members
            .select('concept_id','is_most_recent_version','concept_set_name')
            .where(F.col('is_most_recent_version') == 'true')
            .join(comorbidity_concept_names_df, 'concept_set_name', 'inner')
            .select('concept_id','column_name')
    )

    """ 
    Get all conditions for current set of Covid+ patients    
    where the condition_start_date is not null

    """
    person_conditions_df = (
        condition_occurrence 
            .select('person_id', 'condition_start_date', 'condition_concept_id') 
            .where(F.col('condition_start_date').isNotNull()) 
            .withColumnRenamed('condition_concept_id','concept_id') # renamed for next join
            .join(pf_df,'person_id','inner')
            #.where(F.col('comobidity_start_date') <= F.col('first_poslab_or_diagnosis_date'))  # may want to revist this!!
    )

    # Subset person_conditions_df to records with comorbidities
    person_comorbidities_df = (
        person_conditions_df
            .join(comorbidity_concept_set_members_df, 'concept_id', 'inner')
            .withColumnRenamed('condition_start_date','comorbidity_start_date')
    ) 

    # Transpose column_name (for comorbidities) and create flags for each comorbidity
    person_comorbidities_df = (
        person_comorbidities_df
            .groupby('person_id','comorbidity_start_date')
            .pivot('column_name')
            .agg(F.lit(1)) # flag is 1
            .na.fill(0)    # replace nulls with 0
    )

    return person_comorbidities_df

    

