
# PyAnVIL

## Use Case: Auth terra + gen3

A python client integration of gen3 and terra.

For python developers, who have requirements to access both terra and gen3 platforms, pyAnVIL is an integration module that provides SSO (single sign on) using terra as an IDP (identity provider) and manages distribution of dependencies unlike juggling multiple credentials and installs, pyAnVIL provides developer friendly experience.

### Setup


#### Pre-requisites:

> For terra users, skip this step, terra installs these dependencies in each vm

- gcloud cli tools installed and configured [gcloud](https://cloud.google.com/sdk/install).
- Google Id provisioned in both Terra and Gen3:
  - One time Account Linking:
    - Pre-requisite: google account provisioned in both Gen3 and Terra.

    - Log into https://gen3.theanvil.io/
    - Log into https://anvil.terra.bio
    - In Terra, navigate to your profile
      - Under "IDENTITY & EXTERNAL SERVERS", log into `NHGRI AnVIL Data Commons Framework Services`, the system should present you with a Gen3 Oauth flow.
      - Note the google project used for billing
        ![](docs/_static/terra-profile.png)
    - “unlink” my NHGRI AnVIL Data Commons Framework Services  from https://anvil.terra.bio/#profile 
    - open a new window to gen3.theanvil.io and login using my google id 
    - return to the terra profile screen and “renew” the identity 

- Per instance, terra API setup:
  - Use the google account and billing project to setup credentials for the [terra api](https://github.com/broadinstitute/fiss).
    ```
      gcloud auth login <google-account>
      gcloud auth application-default set-quota-project <billing-project-id>
    ```
- Validation

  ```
  gcloud auth print-access-token
  >>> ya29.a0AfH6SMBSPFSt252qQNl.......

  fissfc config
  >>> ....
  root_url	https://broad-bond-prod.appspot.com/
  ```

- Setup
  ```
  pip install pyAnVIL
  ```

##### SSO

```
   from anvil.gen3_auth import Gen3TerraAuth
   from gen3.submission import Gen3Submission

   auth = Gen3TerraAuth()
   gen3_endpoint = "https://gen3.theanvil.io"
   submission_client = Gen3Submission(gen3_endpoint, auth)
```

[sso sequence diagram](docs/_static/sequence-diagram.png)

##### API Wrappers

###### Gen3

```
   query = '{project(first:0) {code,  subjects {submitter_id}, programs {name}  }}'
   results = submission_client.query(query)
   [p['code'] for p in results['data']['project']]
   >>> ['GTEx', '1000Genomes']
```

###### Terra

```
   from anvil.terra import FAPI
   FAPI.whoami()
   >>> 'anvil.user@gmail.com'
```


#### Data Normalization / FHIR

At this time, AnVIL's terra workspaces contain data from five consortiums spread across 446 workspaces.
These workspaces express study entities in a wide variety of ways: 

Distinct schemas of major entities:

* Patient: (participant) 9, (subject) 32
* Specimen: (sample) 27
* FamilyRelationship: (family) 3
* DocumentReference: (blob) 8
* Task: (sequencing) 24

Note this break down does not account for diversity in vocabularies, entity linking, etc. 

![image](https://user-images.githubusercontent.com/47808/102566809-16b1fc00-4095-11eb-8cf8-f78952ba0464.png)

##### Extract

###### commands

```commandline
# create a working directory for our data
mkdir  -p ./DATA

# manually maintained data tracking 
anvil_etl extract spreadsheet

# gen3 DRS identifiers
anvil_etl extract gen3

# terra workspaces, (takes several minutes) 
anvil_etl extract terra extract 2> /tmp/extract_terra.log
# review /tmp/extract_terra.log
# should end in `INFO     Indexing`
tail /tmp/extract_terra.log

# google blob 
anvil_etl  extract google  --user_project $WORKSPACE_NAMESPACE  2> /tmp/extract_google.log
# review /tmp/extract_google.log
# should end in `INFO     Indexing`
tail /tmp/extract_google.log

```

###### expected results

```text
2022-04-12 01:49:30,144 data_ingestion_tracker.py INFO     Read 485 projects from https://raw.githubusercontent.com/anvilproject/anvil-portal/main/plugins/utils/dashboard-source-anvil.tsv. Wrote to ./DATA/data_ingestion_tracker.json

2022-04-12 01:50:03,672 gen3.py INFO     Created ./DATA/drs_file.sqlite
2022-04-12 01:50:03,822 gen3.py INFO     
Extracted File Counts
gen3_project_id                                     anvil_project_id      file_count
--------------------------------------------------  ------------------  ------------
CCDG-phs001259-DS-CARD-MDS-GSO                                                  4318
CCDG-phs001398-GRU                                                               992
CCDG-phs001487-DS-MULTIPLE_DISEASES-IRB-COL-NPU-RD                              1663
CCDG-phs001569-GRU                                                              2272
CCDG-phs001642-DS-GID                                                            166
CCDG-phs001642-DS-IBD                                                           1462
CCDG-phs001642-GRU                                                              2757
CCDG-phs001642-HMB                                                              1810
CF-GTEx                                                                       122990
CMG-Broad-DS-KRD-RD                                 Hildebrandt                 2444
CMG-Broad-DS-NIC-EMP-LENF                           KNC                          116
CMG-Broad-GRU                                       Bonnemann                    234
CMG-Broad-GRU                                       Manton                       990
CMG-Broad-GRU                                       Pierce                      1274
CMG-Broad-HMB-MDS                                   Gleeson                     2362
CMG-Broad-HMB-MDS                                   Laing                         62
CMG-Broad-HMB-MDS                                   VCGS-White                  1026
CMG-Broad-pre-release-DS-BFD-MDS                    Sankaran                     554
CMG-Broad-pre-release-DS-CSD-MDS                    Seidman                      258
CMG-Broad-pre-release-DS-CVD-MDS                    Ware                          20
CMG-Broad-pre-release-DS-NEURO-GSO-MDS              Beggs                        218
CMG-Broad-pre-release-DS-NEURO-MDS                  Walsh                       1534
CMG-Broad-pre-release-GRU                           Estonia-Ounap                292
CMG-Broad-pre-release-GRU                           OGrady                       146
CMG-Broad-pre-release-HMB-MDS                       Myoseq                      2580
CMG-Broad-pre-release-HMB-MDS                       Ravenscroft                   70
open_access-1000Genomes                                                        13008
tutorial-synthetic_data_set_1                                                  10060

2022-04-12 02:03:48,500 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_WGS')
2022-04-12 02:03:56,872 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_WGSPD1_McCarroll_Escamilla_DS_WGS')
2022-04-12 02:03:57,274 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_10XLRGenomes')
2022-04-12 02:03:58,069 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_WGS')
2022-04-12 02:03:58,784 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_WGS')
2022-04-12 02:03:59,978 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_Finkel_SMA_DS_WGS')
2022-04-12 02:04:00,221 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_10XLRGenomes')
2022-04-12 02:04:00,779 terra.py INFO     ('NIMH', 'AnVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_VillageData')
2022-04-12 02:04:01,959 terra.py INFO     ('Public', '1000G-high-coverage-2019')
2022-04-12 02:04:06,578 entities.py INFO     Indexing

2022-04-12 14:50:34,050 google.py INFO     ('CCDG', 'anvil_ccdg_broad_ai_ibd_daly_xavier_share_wes', 'fc-secure-abc7f058-0260-4e82-a911-abfec3dcb676')
2022-04-12 14:50:34,425 google.py INFO     ('CCDG', 'anvil_ccdg_broad_ai_ibd_niddk_daly_brant_wes', 'fc-secure-29cd113f-7eca-4526-aa52-dde1b8cb41d0')
2022-04-12 14:50:34,978 google.py INFO     ('CCDG', 'anvil_ccdg_broad_ai_ibd_niddk_daly_duerr_wes', 'fc-secure-877e6c8c-72ef-46d0-b3f3-37dd175771fe')
2022-04-12 14:50:35,865 google.py INFO     ('CCDG', 'anvil_ccdg_broad_ai_ibd_niddk_daly_silverberg_wes', 'fc-secure-0eba3dae-89be-4642-8982-9a80a7428cd2')
2022-04-12 14:50:37,030 google.py INFO     ('CCDG', 'anvil_ccdg_broad_daly_igsr_1kg_twist_gsa', 'fc-secure-752e48e6-1e66-4f85-9194-456562e87b90')
2022-04-12 14:50:37,594 google.py INFO     ('CCDG', 'anvil_ccdg_broad_daly_igsr_1kg_twist_wes', 'fc-secure-b41964ad-0c8a-47da-8504-f8636ff3d318')
2022-04-12 14:50:37,980 google.py INFO     ('NHGRI', 'anvil_nhgri_broad_ibd_daly_kugathasan_wes', 'fc-secure-0ca0c5e6-26ca-47ea-b509-ec4eaa058fc6')
2022-04-12 14:50:38,277 google.py INFO     ('NHGRI', 'anvil_nhgri_broad_ibd_daly_turner_wes', 'fc-secure-bee7792c-ef35-478d-a9bb-c8f2054c335c')
2022-04-12 14:50:38,398 google.py INFO     ('NHGRI', 'anvil_nhgri_broad_ibd_daly_winter_wes', 'fc-secure-72a949c5-0b7d-45c9-96c3-ff4d25815ed5')
2022-04-12 14:50:38,675 entities.py INFO     Indexing

```

##### Extract

###### commands

```commandline
# setup environmental values
source /dev/stdin <<< `anvil_etl utility env`  

# normalize the data
anvil_etl transform normalize 2> /tmp/normalize.log
# log should list workspaces, with any warnings or errors logged without exception stack traces.
tail /tmp/normalize.log

# gather statistics
anvil_etl transform analyze 2> /tmp/analyze.log
tail /tmp/analyze.log

# render the qa-report
anvil_etl utility qa > ./DATA/qa-report.md

```

```python
# render the qa-report in a notebook

from IPython.display import Markdown, display, HTML

display(Markdown("./DATA/qa-report.md"))
```

###### expected results

```text

export GOOGLE_PROJECT_NAME=xxx
export GOOGLE_LOCATION=xxx
export GOOGLE_PROJECT=xxx
export GOOGLE_DATASET=xxx
export TOKEN=xxx
export GOOGLE_DATASTORES=xxx
export GOOGLE_DATASTORE=xxx
export GOOGLE_BUCKET=xxx
export OUTPUT_PATH=xxx
export IMPLEMENTATION_GUIDE_PATH=xxx


2022-04-12 15:05:34,105 normalizer.py INFO     ('CCDG', 'anvil_ccdg_broad_ai_ibd_niddk_daly_silverberg_wes')
2022-04-12 15:05:36,168 normalizer.py INFO     ('CCDG', 'anvil_ccdg_broad_daly_igsr_1kg_twist_gsa')
2022-04-12 15:05:36,899 normalizer.py INFO     ('CCDG', 'anvil_ccdg_broad_daly_igsr_1kg_twist_wes')
2022-04-12 15:05:37,483 normalizer.py INFO     ('NHGRI', 'anvil_nhgri_broad_ibd_daly_kugathasan_wes')
2022-04-12 15:05:37,939 normalizer.py INFO     ('NHGRI', 'anvil_nhgri_broad_ibd_daly_turner_wes')
2022-04-12 15:05:38,096 normalizer.py INFO     ('NHGRI', 'anvil_nhgri_broad_ibd_daly_winter_wes')


2022-04-19 00:31:47,216 transform.py INFO     working on anvil_nhgri_broad_ibd_daly_kugathasan_wes
2022-04-19 00:31:47,237 transform.py INFO     working on anvil_nhgri_broad_ibd_daly_turner_wes
2022-04-19 00:31:47,290 transform.py INFO     working on anvil_nhgri_broad_ibd_daly_winter_wes

```



|     | consortium   | workspace                                                       |   patients |   specimens |   tasks |   documents |   vcf |   tbi |   cram |   qa_grade |   drs_grade |   md5 |   crai |   idat |   gtc |   NA |   bam |   bai |   bedpe |   loupe |   csv |   txt |   nan |
|-----|--------------|-----------------------------------------------------------------|------------|-------------|---------|-------------|-------|-------|--------|------------|-------------|-------|--------|--------|-------|------|-------|-------|---------|---------|-------|-------|-------|
|   0 | Public       | 1000G-high-coverage-2019                                        |       3202 |        3202 |    3202 |        9609 |  3205 |  3202 |   3202 |    99.9994 |     99.0006 |       |        |        |       |      |       |       |         |         |       |       |       |
|   1 | CMG          | ANVIL_CMG_BROAD_BRAIN_ENGLE_WES                                 |        946 |         946 |     946 |        1419 |       |       |    473 |   100      |      0      |   473 |    473 |        |       |      |       |       |         |         |       |       |       |
|   2 | CMG          | ANVIL_CMG_BROAD_BRAIN_SHERR_WGS                                 |          6 |           6 |       6 |           9 |       |       |      3 |   100      |      0      |     3 |      3 |        |       |      |       |       |         |         |       |       |       |
|   3 | CMG          | ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS                                |         30 |          30 |      30 |          45 |       |       |     15 |   100      |      0      |    15 |     15 |        |       |      |       |       |         |         |       |       |       |
|   4 | CMG          | ANVIL_CMG_Broad_Muscle_Laing_WES                                |         31 |          31 |      31 |          62 |       |       |     31 |    99.9355 |     99      |       |     31 |        |       |      |       |       |         |         |       |       |       |
|   5 | CMG          | ANVIL_CMG_Broad_Orphan_Jueppner_WES                             |         20 |          20 |      20 |          30 |       |       |     10 |   100      |      0      |    10 |     10 |        |       |      |       |       |         |         |       |       |       |
|   6 | CMG          | ANVIL_CMG_UWASH_DS-BAV-IRB-PUB-RD                               |        177 |         177 |       0 |           0 |       |       |        |    99.9774 |      0      |       |        |        |       |      |       |       |         |         |       |       |       |
 | ... |
| 445 | NHGRI        | anvil_nhgri_broad_ibd_daly_winter_wes                           |        823 |         823 |     823 |        1236 |       |       |    412 |   100      |      0      |   412 |    412 |        |       |      |       |       |         |         |       |       |       |




##### Load

###### commands


* See the load options on `anvil_etl load fhir`

```
$ anvil_etl load fhir --help

Usage: anvil_etl load fhir [OPTIONS] COMMAND [ARGS]...

  Commands to setup and load fhir server.

Options:
  --help  Show this message and exit.

Commands:
  IG          Commands to create and delete implementation guide.
  data-set    Commands to create and delete data_set.
  data-store  Commands to create and delete data_store.
```



```commandline
# create the IG, FHIR's "schema"
anvil_etl load fhir IG create
# create the data set and data store containers
anvil_etl load fhir data-set create
anvil_etl load fhir data-store create
# load the data to respective stores
anvil_etl load fhir data-store load
# load all public resources into the public store
anvil_etl load fhir data-store load-public
```

###### expected results

* You can view progress at https://console.cloud.google.com/healthcare/browser/locations/us-west2/datasets/anvil-test/operations?project=fhir-test-16-342800



##### Query

###### Access: Permissions


Roles are assigned at the **data set** level

![image](https://user-images.githubusercontent.com/47808/164029675-43cb8a7c-54ee-4bef-bda0-53dc90eaaf4b.png)


and are inherited by child **data-stores**:

![image](https://user-images.githubusercontent.com/47808/164029728-fc59b2b9-7181-4d7a-a564-d6ec64c08469.png)


###### REST API
  * See FHIR's [Search API](https://www.hl7.org/fhir/search.html)
  * See Google's [Healthcare API conformance statement](https://cloud.google.com/healthcare-api/docs/fhir#r4)

  * The base url is: `https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores`

  * Append data-store to this for a complete url e.g. `public/fhir/` : 
> https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir/


* Complete data-store list:

  * pending
  * phs000160-Consortia_Access_Only
  * phs000298
  * phs000298-DS-ASD
  * phs000298-DS-MH
  * phs000298-GRU
  * phs000298-GRU-NPU
  * phs000298-HMB
  * phs000356-HMB-NPU
  * phs000424-GRU
  * phs000496-Consortia_Access_Only
  * phs000693-DS-BAV-IRB-PUB-PD
  * phs000693-DS-BDIS
  * phs000693-DS-EP
  * phs000693-DS-HFA
  * phs000693-DS-NBIA
  * phs000693-GRU
  * phs000693-GRU-IRB
  * phs000693-HMB
  * phs000693-HMB-IRB
  * phs000711-HMB-IRB-NPU
  * phs000711-HMB-NPU
  * phs000744-DS-MC
  * phs000744-DS-RARED
  * phs000744-GRU
  * phs000744-HMB
  * phs000744-HMB-GSO
  * phs000920-DS-LD-RD
  * phs000997
  * phs000997-Consortia_Access_Only
  * phs001062
  * phs001062-Consortia_Access_Only
  * phs001155-GRU
  * phs001211-HMB-IRB
  * phs001222-DS-DRC-IRB-NPU
  * phs001227-DS-ATHSCL-IRB-MDS
  * phs001227-GRU-IRB
  * phs001259-DS-CARD-MDS-GSO
  * phs001272
  * phs001272-Consortia_Access_Only
  * phs001272-DS-BFD-MDS
  * phs001272-DS-CSD-MDS
  * phs001272-DS-NEURO-GSO-MDS
  * phs001272-GRU
  * phs001272-HMB-MDS
  * phs001395-HMB-NPU
  * phs001398-GRU
  * phs001487-DS-CVD-IRB-COL-MDS
  * phs001489
  * phs001489-DS-CARDI_NEURO
  * phs001489-DS-EAED-MDS
  * phs001489-DS-EARET-MDS
  * phs001489-DS-EP
  * phs001489-DS-EP-ETIOLOGY-MDS
  * phs001489-DS-EP-MDS
  * phs001489-DS-EP-NPU
  * phs001489-DS-EPI-ASZ-MED-MDS
  * phs001489-DS-EPI-MUL-CON-MDS
  * phs001489-DS-EPI-NPU-MDS
  * phs001489-DS-EPIL-BC-ID-MDS
  * phs001489-DS-NEURO-AD-NPU
  * phs001489-DS-NEURO-MDS
  * phs001489-DS-NPD-IRB-NPU
  * phs001489-DS-SEIZD
  * phs001489-EPIL_BRAINAB_CONVUL_INTELCT_DIS_MDS
  * phs001489-EPIL_BRAIN_AB_INTEL_DIS_MDS
  * phs001489-EPIL_BRAIN_AB_MDS
  * phs001489-EPIL_CO_MORBIDI_MDS
  * phs001489-GRU
  * phs001489-GRU-IRB
  * phs001489-GRU-NPU
  * phs001489-HMB
  * phs001489-HMB-IRB-MDS
  * phs001489-HMB-MDS
  * phs001489-HMB-NPU
  * phs001489-HMB-NPU-MDS
  * phs001498
  * phs001502
  * phs001502-Consortia_Access_Only
  * phs001502-HMB-IRB-PUB
  * phs001506-DS-CVD-IRB
  * phs001543-Consortia_Access_Only
  * phs001544-Consortia_Access_Only
  * phs001545-Consortia_Access_Only
  * phs001546
  * phs001547-Consortia_Access_Only
  * phs001569-Consortia_Access_Only
  * phs001569-GRU
  * phs001579-GRU-IRB-NPU
  * phs001592-DS-CVD
  * phs001598-Consortia_Access_Only
  * phs001600
  * phs001600-Consortia_Access_Only
  * phs001624
  * phs001624-Consortia_Access_Only
  * phs001624-HMB-GSO
  * phs001642
  * phs001642-DS-GID
  * phs001642-DS-IBD
  * phs001642-GRU
  * phs001642-HMB
  * phs001644
  * phs001676-DS-AONDD-IRB
  * phs001725
  * phs001740-DS-ASD-IRB
  * phs001741-DS-ASD-IRB
  * phs001766-DS-ASD
  * phs001766-DS-ASD-IRB
  * phs001871-DS-CAD-IRB-COL-NPU
  * phs001873-HMB-GSO
  * phs001880-GRU-NPU
  * phs001894-DS-EAC-PUB-GSO
  * phs001913-GRU-IRB
  * phs001933
  * phs001933-Consortia_Access_Only
  * phs002004-DS-ASD
  * phs002018-Consortia_Access_Only
  * phs002018-HMB
  * phs002032-GRU
  * phs002041-DS-MLHLTH-MDS
  * phs002041-DS-SZRD-MDS
  * phs002041-GRU
  * phs002042-DS-ASD-MDS-PUB
  * phs002042-GRU-MDS-PUB
  * phs002043-DS-ASD
  * phs002043-GRU
  * phs002044-DS-ASD-IRB
  * phs002163-GRU
  * phs002236
  * phs002242
  * phs002243-HMB
  * phs002282-DS-CVDRF
  * phs002325-DS-CVD
  * phs002726
  * phs002774
  * public

### Research Study discovery
* The `fhir_curl` command will dispatch to all stores and discover `ResearchStudy` entities:

```
export TOKEN=$(gcloud auth application-default print-access-token)
export GOOGLE_DATASTORES=$(gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION | awk '(NR>1){print $1}' | sed  's/$/,/g' | tr -d "\n")
fhir_curl '/ResearchStudy?_elements=id&_count=1000'  | jq -rc '.entry[] | .fullUrl' | sort 

https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-ac-boston-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-AGRE-FEMF-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-domenici-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-herman-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-lattig-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-mcpartland-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-minshew-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-palotie-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-puura-asd-exome
https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/pending/fhir/ResearchStudy/AnVIL-ccdg-asc-ndd-daly-talkowski-TASC-asd-exome
...
```


## Contributing

* Setup Environment

Set environmental variables by calling `fhir_env`  Provide a project name and region.  Note: please ensure the healthcare API is available in that region. https://cloud.google.com/healthcare-api/docs/concepts/regions

The script will set reasonable values for other environmental variables.  You may override them on the command line.

> usage: fhir_env GOOGLE_PROJECT_NAME GOOGLE_LOCATION [GOOGLE_DATASET] [GOOGLE_DATASTORE] [BILLING_ACCOUNT] [GOOGLE_APPLICATION_CREDENTIALS] [SPREADSHEET_UUID] [OUTPUT_PATH]


```
$ source fhir_env fhir-test-16 us-west2
***** env variables *****
GOOGLE_PROJECT_NAME fhir-test-16 The root for the API, billing, buckets, etc.
GOOGLE_BILLING_ACCOUNT XXXXXX-XXXX-XXXX Google Cloud Billing Accounts allow you to configure payment and track spending in GCP.
GOOGLE_LOCATION us-west2 The physical location of the data
GOOGLE_DATASET anvil-test Datasets are top-level containers that are used to organize and control access to your stores.
GOOGLE_DATASTORE test A FHIR store is a data store in the Cloud Healthcare API that holds FHIR resources.
OUTPUT_PATH ./DATA A directory on your local system, used to store work files.
GOOGLE_PROJECT fhir-test-16-342800 The project identifier
GOOGLE_BUCKET fhir-test-16-342800 The bucket used to store extracted FHIR files
```


We incorporated `fhirclient`, a flexible Python client for FHIR servers supporting the SMART on FHIR protocol. 

Note: You will need to install the fhir client separately.  see  https://github.com/smart-on-fhir/client-py/issues/70

```
pip install  fhirclientr4
```

Example

```

from anvil.fhir.client import FHIRClient
from anvil.fhir.smart_auth import GoogleFHIRAuth
settings = {
    'app_id': 'my_web_app',
    'api_base': 'https://healthcare.googleapis.com/v1beta1/projects/fhir-test-16-342800/locations/us-west2/datasets/anvil-test/fhirStores/public/fhir'
}
smart = FHIRClient(settings=settings)
# optionally pass token
# smart.server.auth = GoogleFHIRAuth(access_token='ya29.abcd...')
smart.server.auth = GoogleFHIRAuth()
smart.prepare()
assert smart.ready, "server should be ready"
# search for all ResearchStudy
import fhirclient.models.researchstudy as rs
[s.title for s in rs.ResearchStudy.where(struct={}).perform_resources(smart.server)]
>>> 
['1000g-high-coverage-2019', 'my NCPI research study example']

```

For more information on usage see [smart-on-fhir/client-py](https://github.com/smart-on-fhir/client-py)


Local testing

Test json files using FHIR reference validator 

```commandline
java -jar validator_cli.jar  /tmp/invalid_body_no_subject.json -ig ~/client-apis/pyAnVIL/DATA/fhir/IG/
```
 


## Contributing

- set up virtual env

  ```
  python3 -m venv venv
  source venv/bin/activate
  python3 -m pip install -r requirements.txt
  python3 -m pip install -r requirements-dev.txt
  ```

- test gen3 authorization

  ```
  python3 -m pytest --user_email <GMAIL ACCOUNT>  --log-level DEBUG  --gen3_endpoint <GEN3_ENDPOINT>  tests/integration/test_gen3_auth.py
  ```


### Distribution

- PyPi

```
# update pypi

export TWINE_USERNAME=  #  the username to use for authentication to the repository.
export TWINE_PASSWORD=  # the password to use for authentication to the repository.

rm -r dist/
python3  setup.py sdist bdist_wheel
twine upload dist/*
```

- Read The Docs

```
https://readthedocs.org/projects/pyanvil/
```
