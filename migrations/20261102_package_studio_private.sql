-- Owner-only draft storage; this does not enable or change any product.
BEGIN;
CREATE TABLE IF NOT EXISTS public.package_studio_private_settings (
 key text PRIMARY KEY,
 value text NOT NULL CHECK (octet_length(value) <= 1048576),
 updated_at timestamptz NOT NULL DEFAULT now(),
 updated_by text
);
ALTER TABLE public.package_studio_private_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.package_studio_private_settings FORCE ROW LEVEL SECURITY;
REVOKE ALL ON public.package_studio_private_settings FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON public.package_studio_private_settings TO service_role;
NOTIFY pgrst, 'reload schema';
COMMIT;
