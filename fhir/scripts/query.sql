// No samples with no sequencing
create table missing_sequencing (
    key text,
    submitter_id text
) ;

insert into missing_sequencing
select s.key, s.submitter_id  from vertices  as s
where s.name = 'sample' 
and
not EXISTS(
    select q.src from edges as q where q.dst = s.key 
) ;


// No subjects with samples with no sequencing
create table subjects_missing_sequencing (
    key text,
    submitter_id text
) ;

insert into subjects_missing_sequencing
select s.key, s.submitter_id  from vertices  as s
where s.name = 'subject' 
and s.key in
(
    select q.dst from edges as q where q.src in (select ms.key from missing_sequencing as ms)
) ;

select 
edges.dst 
missing_sequencing.key ,
missing_sequencing.missing_sequencing.key  
from missing_sequencing join on edges where missing_sequencing.key = edges.src ;


-- has sequencing
select 
    json_extract(su.json, '$.object.project_id'),
    json_extract(su.json, '$.object.anvil_project_id'),
    su.name,
    su.key as "subject_id",
    json_extract(su.json, '$.object.participant_id'),
    json_extract(su.json, '$.object.submitter_id'),
    json_extract(su.json, '$.object.project_id'),
    sa.name,
    sa.key  as "sample_id",
    json_extract(su.json, '$.object.sample_id'),
    json_extract(su.json, '$.object.submitter_id'),
    'sequencing',
    sequencing_edge.src  as "sequencing_id"
    from vertices as su 
        join edges as sample_edge on sample_edge.dst = su.key
            join vertices as sa on sample_edge.src = sa.key 
                join edges as sequencing_edge on sequencing_edge.dst = sa.key
    where su.name = 'subject'            
    limit 10 ;

-- missing sequencing
drop table if exists flattened ;
create table flattened
as
select
    json_extract(su.json, '$.object.project_id') as "project_id",
    json_extract(su.json, '$.object.anvil_project_id') as "anvil_project_id",
    su.name as "subject_type",
    su.key as "subject_id",
    json_extract(su.json, '$.object.participant_id') as "participant_id",
    json_extract(su.json, '$.object.submitter_id') as "subject_submitter_id",
    sa.name as "sample_type",
    sa.key  as "sample_id",
    json_extract(sa.json, '$.object.sample_id') as "sample_sample_id",
    json_extract(sa.json, '$.object.submitter_id') as "sample_submitter_id",
    json_extract(sa.json, '$.object.specimen_id') as "sample_specimen_id",
    'sequencing' as "sequencing_type",
    sequencing_edge.src  as "sequencing_id",
    json_extract(sq.json, '$.object.submitter_id') as "sequencing_submitter_id",
    json_extract(sq.json, '$.object.ga4gh_drs_uri') as "ga4gh_drs_uri"
    from vertices as su 
        join edges as sample_edge on sample_edge.dst = su.key and sample_edge.src_name = 'sample'
            join vertices as sa on sample_edge.src = sa.key  
                left join edges as sequencing_edge on sequencing_edge.dst = sa.key and sequencing_edge.src_name = 'sequencing'
                    join vertices as sq on sequencing_edge.src = sq.key 

    where           
    su.name = 'subject'            ;


drop table if exists summary ;
create table summary
as
 select f.project_id, f.anvil_project_id, 
    count(distinct f.subject_id) as "subject_count", 
    count(distinct f.sample_id) as "sample_count",
    count(distinct m.sequencing_id) as "sequencing_count",
    count(distinct m.ga4gh_drs_uri) as "ga4gh_drs_uri_count"
    from flattened as f
        left join flattened as m on f.project_id = m.project_id and f.anvil_project_id = m.anvil_project_id
    group by f.project_id, f.anvil_project_id;


drop table if exists reconcile_counts;
create table reconcile_counts as 
select w.workspace_id,
    count(distinct w.sample_id) as "terra_sample_id_count",
    count(distinct f.sample_submitter_id) as "gen3_sample_id_count",
    count(distinct w.blob) as "terra_blob_count",
    count(distinct f.ga4gh_drs_uri) as "gen3_drs_uri_count"
    from terra_details as w 
        left join flattened as f on (w.sample_id || '_sample' = f.sample_submitter_id)
group by w.workspace_id    
having gen3_sample_id_count > 0 
UNION
select w.workspace_id,
    count(distinct w.sample_id) as "terra_sample_id_count",
    count(distinct f.sample_submitter_id) as "gen3_sample_id_count",
    count(distinct w.blob) as "terra_blob_count",
    count(distinct f.ga4gh_drs_uri) as "gen3_drs_uri_count"
    from terra_details as w 
        left join flattened as f on (w.sample_id   = f.sample_submitter_id)
group by w.workspace_id    
having gen3_sample_id_count > 0 
UNION
select w.workspace_id,
    count(distinct w.sample_id) as "terra_sample_id_count",
    count(distinct f.sample_submitter_id) as "gen3_sample_id_count",
    count(distinct w.blob) as "terra_blob_count",
    count(distinct f.ga4gh_drs_uri) as "gen3_drs_uri_count"
    from terra_details as w 
        left join flattened as f on (w.sample_id   = f.sample_specimen_id)
group by w.workspace_id    
having gen3_sample_id_count > 0 
;

insert into reconcile_counts
select w.workspace_id,
    count(distinct w.sample_id) as "terra_sample_id_count",
    0 as "gen3_sample_id_count",
    count(distinct w.blob) as "terra_blob_count",
    0 as "gen3_drs_uri_count"
from terra_details  as w
where workspace_id not in ( select distinct workspace_id from reconcile_counts ) 
group by w.workspace_id    
;