from .bioontology_lookup import lookup_term

raw_terms = """
    Adipose - Subcutaneous
    Adipose - Visceral (Omentum)
    Adrenal Gland
    Artery - Aorta
    Artery - Coronary
    Artery - Tibial
    Bladder
    Brain - Amygdala
    Brain - Anterior cingulate cortex (BA24)
    Brain - Caudate (basal ganglia)
    Brain - Cerebellar Hemisphere
    Brain - Cerebellum
    Brain - Cortex
    Brain - Frontal Cortex (BA9)
    Brain - Hippocampus
    Brain - Hypothalamus
    Brain - Nucleus accumbens (basal ganglia)
    Brain - Putamen (basal ganglia)
    Brain - Spinal cord (cervical c-1)
    Brain - Substantia nigra
    Breast - Mammary Tissue
    Cells - Cultured fibroblasts
    Cells - EBV-transformed lymphocytes
    Cervix - Ectocervix
    Cervix - Endocervix
    Colon - Sigmoid
    Colon - Transverse
    Esophagus - Gastroesophageal Junction
    Esophagus - Mucosa
    Esophagus - Muscularis
    Fallopian Tube
    Heart - Atrial Appendage
    Heart - Left Ventricle
    Kidney - Cortex
    Kidney - Medulla
    Liver
    Lung
    Minor Salivary Gland
    Muscle - Skeletal
    Nerve - Tibial
    Ovary
    Pancreas
    Pituitary
    Prostate
    Skin - Not Sun Exposed (Suprapubic)
    Skin - Sun Exposed (Lower leg)
    Small Intestine - Terminal Ileum
    Spleen
    Stomach
    Testis
    Thyroid
    Uterus
    Vagina
    Whole Blood
""".split("\n")

ontology_text = {'http://github.com/obophenotype/uberon/UBERON_0002190': 'Adipose - Subcutaneous',
                 'http://github.com/obophenotype/uberon/UBERON_0014454': 'Adipose - Visceral (Omentum)',
                 'http://github.com/obophenotype/uberon/UBERON_0002369': 'Adrenal Gland',
                 'http://github.com/obophenotype/uberon/UBERON_0012254': 'Artery - Aorta',
                 'http://github.com/obophenotype/uberon/UBERON_0001621': 'Artery - Coronary',
                 'http://github.com/obophenotype/uberon/UBERON_0007610': 'Artery - Tibial',
                 'http://github.com/obophenotype/uberon/UBERON_0018707': 'Bladder',
                 'http://github.com/obophenotype/uberon/UBERON_0001876': 'Brain - Amygdala',
                 'http://github.com/obophenotype/uberon/UBERON_0009835': 'Brain - Anterior cingulate cortex (BA24)',
                 'http://github.com/obophenotype/uberon/UBERON_0010011': 'Brain - Putamen (basal ganglia)',
                 'http://github.com/obophenotype/uberon/UBERON_0002245': 'Brain - Cerebellar Hemisphere',
                 'http://github.com/obophenotype/uberon/UBERON_0002037': 'Brain - Cerebellum',
                 'http://github.com/obophenotype/uberon/UBERON_0001851': 'Brain - Cortex',
                 'http://github.com/obophenotype/uberon/UBERON_0001870': 'Brain - Frontal Cortex (BA9)',
                 'http://github.com/obophenotype/uberon/UBERON_0001898': 'Brain - Hypothalamus',
                 'http://github.com/obophenotype/uberon/UBERON_0001882': 'Brain - Nucleus accumbens (basal ganglia)',
                 'http://github.com/obophenotype/uberon/UBERON_0002726': 'Brain - Spinal cord (cervical c-1)',
                 'http://github.com/obophenotype/uberon/UBERON_0002038': 'Brain - Substantia nigra',
                 'http://github.com/obophenotype/uberon/UBERON_0003584': 'Breast - Mammary Tissue',
                 'http://github.com/obophenotype/uberon/UBERON_0006591': 'Cells - EBV-transformed lymphocytes',
                 'http://github.com/obophenotype/uberon/UBERON_0012249': 'Cervix - Ectocervix',
                 'http://github.com/obophenotype/uberon/UBERON_0000458': 'Cervix - Endocervix',
                 'http://github.com/obophenotype/uberon/UBERON_0001159': 'Colon - Sigmoid',
                 'http://github.com/obophenotype/uberon/UBERON_0001157': 'Colon - Transverse',
                 'http://github.com/obophenotype/uberon/UBERON_0002469': 'Esophagus - Mucosa',
                 'http://github.com/obophenotype/uberon/UBERON_0004648': 'Esophagus - Muscularis',
                 'http://github.com/obophenotype/uberon/UBERON_0003889': 'Fallopian Tube',
                 'http://github.com/obophenotype/uberon/UBERON_0002084': 'Heart - Left Ventricle',
                 'http://github.com/obophenotype/uberon/UBERON_0001225': 'Kidney - Cortex',
                 'http://github.com/obophenotype/uberon/UBERON_0001294': 'Kidney - Medulla',
                 'http://github.com/obophenotype/uberon/UBERON_0002107': 'Liver',
                 'http://github.com/obophenotype/uberon/UBERON_0002048': 'Lung',
                 'http://github.com/obophenotype/uberon/UBERON_0001830': 'Minor Salivary Gland',
                 'http://github.com/obophenotype/uberon/UBERON_0001323': 'Nerve - Tibial',
                 'http://github.com/obophenotype/uberon/UBERON_0000992': 'Ovary',
                 'http://github.com/obophenotype/uberon/UBERON_0001264': 'Pancreas',
                 'http://github.com/obophenotype/uberon/UBERON_0000007': 'Pituitary',
                 'http://github.com/obophenotype/uberon/UBERON_0002367': 'Prostate',
                 'http://github.com/obophenotype/uberon/UBERON_0036149': 'Skin - Not Sun Exposed (Suprapubic)',
                 'http://github.com/obophenotype/uberon/UBERON_0004264': 'Skin - Sun Exposed (Lower leg)',
                 'http://github.com/obophenotype/uberon/UBERON_0002108': 'Small Intestine - Terminal Ileum',
                 'http://github.com/obophenotype/uberon/UBERON_0002106': 'Spleen',
                 'http://github.com/obophenotype/uberon/UBERON_0000945': 'Stomach',
                 'http://github.com/obophenotype/uberon/UBERON_0000473': 'Testis',
                 'http://github.com/obophenotype/uberon/UBERON_0002046': 'Thyroid',
                 'http://github.com/obophenotype/uberon/UBERON_0000995': 'Uterus',
                 'http://github.com/obophenotype/uberon/UBERON_0000996': 'Vagina',
                 'http://github.com/obophenotype/uberon/UBERON_0000178': 'Whole Blood'}

# manual lookup
ontology_text['http://github.com/obophenotype/uberon/UBERON_0004857'] = 'Muscle - Skeletal'
ontology_text['http://github.com/obophenotype/uberon/UBERON_0006618'] = 'Heart - Atrial Appendage'
ontology_text['http://github.com/obophenotype/uberon/UBERON_0007650'] = 'Esophagus - Gastroesophageal Junction'
ontology_text['http://www.ebi.ac.uk/efo/EFO_0002009'] = 'Cells - Cultured fibroblasts'
ontology_text['http://github.com/obophenotype/uberon/UBERON_0001873'] = 'Brain - Caudate (basal ganglia)'
ontology_text['http://github.com/obophenotype/uberon/UBERON_0001954'] = 'Brain - Hippocampus'

text_ontology = {v: k for k, v in ontology_text.items()}


def lookup_body_site(term) -> (str, str):
    """Return (system, code)."""
    assert term in text_ontology, f"Could not find body_site: >{term}<"
    system_parts = text_ontology[term].split('/')
    return '/'.join(system_parts[:-1]), system_parts[-1]


def lookup_raw_terms():
    """Lookup raw terms."""
    ontology_text_ = {}
    raw_terms = [t.strip(' ') for t in raw_terms if len(t) > 1]

    for term in raw_terms:
        items = [item for ontology_preference, item in lookup_term(term, ontology_preferences=['UBERON'])]
        if len(items) == 0:
            continue
        item = items[0]
        ontology_text_[item['@id']] = term

    print(ontology_text_)


if __name__ == "__main__":
   lookup_raw_terms()