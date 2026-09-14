CREATE TABLE sgsp_group_assignments (
    app_id text NOT NULL,
    group_key text NOT NULL,
    app_version text NOT NULL,
    owner_id text NOT NULL,
    incarnation bytea NOT NULL CHECK (octet_length(incarnation) = 16),
    endpoint_address text NOT NULL,
    endpoint_server_name text NOT NULL,
    closed boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    PRIMARY KEY (app_id, group_key),
    CHECK (closed = (closed_at IS NOT NULL))
);
