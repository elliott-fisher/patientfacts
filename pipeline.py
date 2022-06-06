

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55"),
    covid_pos_sample=Input(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    person_lds=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
def covid_pos_person(covid_pos_sample, location, manifest, person_lds):
    person = person_lds
    
    df = (
        covid_pos_sample.join(
            person_lds.select(  'person_id','year_of_birth','month_of_birth','day_of_birth',
                                'ethnicity_concept_name','race_concept_name','gender_concept_name',
                                'location_id','data_partner_id'),
            covid_pos_sample.person_id == person_lds.person_id,
            how = "left"
        ).drop(person_lds.person_id) 
    )
    

    df = (
        df.join(
            location.select('location_id','city','state','zip','county'),
            df.location_id == location.location_id,
            how = "left"    
        ).drop(location.location_id)
    )

    return df

"""
    df = (
        df.join(
            manifest.select('data_partner_id','run_date','cdm_name','cdm_version','shift_date_yn','max_num_shift_days'),
            df.
        )

    )
"""
# location: 'location_id','city','state','zip','county'
# manaifest data_partner_id','run_date','cdm_name','cdm_version','shift_date_yn','max_num_shift_days

            

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51"),
    ALL_COVID_POS_PERSONS=Input(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903")
)
def covid_pos_sample(ALL_COVID_POS_PERSONS):

    proportion_of_patients_to_use = .001

    return ALL_COVID_POS_PERSONS.sample(False, proportion_of_patients_to_use, 111)
    

