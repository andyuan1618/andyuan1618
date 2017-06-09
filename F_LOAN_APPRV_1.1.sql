/**
  *-----------------------------Dianrong BI Team-----------------------------
  *		Project Name:		DW_ETL
  *		File Name:			f_loan_apprv.sql
  *		Exec Freq:			Daily
  *		Create Date:		2017-6-8
  *		Version: 			1.1
  *		Author:				Gang Yuan
  *		Function: 			Load incremental data into dw.f_loan_apprv.
  */

/* create table sql */

create table dw.f_loan_apprv (
   appl_id              bigint comment '申请id',
   loan_rejct_code      string comment '拒绝编码',
   loan_prod_id         string comment '贷款产品id(uuid)',
   borwg_chanl_id       int comment '渠道id',
   apprv_date_key       int comment '审批日期键',
   dr_user_id           string comment '虚拟出来的账户id',
   loan_apprv_persn_id  string comment '贷款审核人id（uuid）',
   salesm_skid          string comment '销售员代理键(uuid)',
   apprv_type           int comment '1为默认审批，2为人工初审，3为人工终审，4为自动审批',
   is_apprv_pass_flg    int comment '是否审批通过标志，1为是，0为拒绝',
   appl_amt             decimal(18, 6) comment '申请借款金额',
   srce_creat_time      timestamp comment '源系统创建时间',
   srce_updt_time       timestamp comment '源系统更新时间',
   apprv_time			timestamp comment '审批时间',
   dw_audit_cre_date	string comment '分区日期',
   etl_batch_id         bigint comment 'etl批次id'
)
   partitioned by (dw_audit_cre_date)
   stored as orc
   tblproperties ("orc.compress"="SNAPPY")
   comment '贷款审批的条件事实表';

/* load transition table */

insert overwrite table dw.w_loan_apprv
select  h.loan_app_id									as appl_id,
 		a.reject_code									as loan_rejct_code,
 		b.loan_prod_id    								as loan_prod_id,
 		a.channel_id									as borwg_chanl_id,
 		g.date_key										as apprv_date_key,
 		c.dr_user_id 									as dr_user_id,
 		'-1'											as loan_apprv_persn_id,
 		e.salesm_skid									as salesm_skid,
 		1												as apprv_type,
 		case when h.cur_status = 5 then 1 else 0 end	as is_apprv_pass_flg,
 		a.app_amount									as appl_amt,
 		a.gmt_create 									as srce_creat_time,
		a.gmt_update 									as srce_updt_time,
		h.gmt_change 									as apprv_time,
		date_format(h.gmt_change, 'yyyy-MM-dd')			as dw_audit_cre_date,
		null											as etl_batch_id

from (
		select  loan_app_id, 
				cur_status, 
				gmt_change, 
				row_number() over(partition by loan_app_id, cur_status order by gmt_change desc) as rnk 
		from acrc.v_loan_app_status_log
		where cur_status in (3, 5)
		) h
		
left join acrc.v_loan_app a
on h.loan_app_id = a.id

left join dw.d_date g
on to_date(h.gmt_change) = to_date(g.date_time_start)

left join dw.d_loan_prod b
on a.product_id = b.srce_loan_prod_id
 
left join dw.d_dr_user_activ c
on a.user_id = c.app_user_id

left join acrc.v_loan_app_refer d
on a.id = d.loan_app_id
 
left join dw.d_salesm e
on d.refer_code = e.salesm_id

where h.rnk = 1 and h.gmt_change between date_add(current_date(), -3) and current_date();



/* set dynamic partition parameter */

set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

/* load data into target table from transition table */

insert overwrite table dw.f_loan_apprv partition(dw_audit_cre_date)
select  appl_id,
 		loan_rejct_code,
 		loan_prod_id,
 		borwg_chanl_id,
 		apprv_date_key,
 		dr_user_id,
 		loan_apprv_persn_id,
 		salesm_skid,
 		apprv_type,
 		is_apprv_pass_flg,
 		appl_amt,
 		srce_creat_time,
		srce_updt_time,
		apprv_time,
		dw_audit_cre_date
		etl_batch_id
from dw.w_loan_apprv;





