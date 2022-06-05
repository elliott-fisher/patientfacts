

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51"),
    ALL_COVID_POS_PERSONS=Input(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903")
)
def patient_sample(ALL_COVID_POS_PERSONS):

    proportion_of_patients_to_use = .001

    return = ALL_COVID_POS_PERSONS.sample(False, proportion_of_patients_to_use, 111)
    

