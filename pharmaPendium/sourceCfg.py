from enum import Enum


class Source(Enum):
    EFFICACY = 'efficacy'
    DRUG_SAFETY = 'safety'
    SAFETY_AERS = 'reports'
    PK_DATA = 'pk'
    FAERS = 'faers'


SOURCE_CFG = {
    Source.EFFICACY: {
        "taxonomyTreeType": ["Drugs", "Targets", "Indications"]
    },
    Source.DRUG_SAFETY: {
        "taxonomyTreeType": ["Drugs", "Targets", "Indications", "Effects", "Sources"]
    }

}
