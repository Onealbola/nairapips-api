-- WITHDRAWN: plan changes apply only to new purchases.
-- Never upgrade historical purchases or reopen completed businesses.
-- Use 20261117_restore_existing_purchase_cycles.sql to undo the audited upgrade.
DO $$ BEGIN RAISE EXCEPTION 'Withdrawn historical upgrade. Plan changes apply to new purchases only.'; END $$;
