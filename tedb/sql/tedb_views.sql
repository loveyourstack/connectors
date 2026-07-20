
CREATE OR REPLACE VIEW tedb.v_vat_rate AS
  SELECT
    tedb_vr.id,
    tedb_vr.category_fk,
      tedb_vc.identifier AS category_identifier,
    tedb_vr.cn_codes,
    tedb_vr.comment,
    tedb_vr.cpa_codes,
    tedb_vr.created_at,
    tedb_vr.member_state,
    tedb_vr.rate_type,
    tedb_vr.rate,
    tedb_vr.situation_on,
    tedb_vr.type,
    tedb_vr.updated_at
  FROM tedb.vat_rate tedb_vr
  JOIN tedb.vat_category tedb_vc ON tedb_vr.category_fk = tedb_vc.id;