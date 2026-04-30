
CREATE DOMAIN int_gte0 AS integer NOT NULL CHECK (value >= 0);
CREATE DOMAIN int_positive AS integer NOT NULL CHECK (value > 0);
CREATE DOMAIN text_short_mandatory AS varchar(64) NOT NULL CHECK (value != '');
CREATE DOMAIN tracking_at AS timestamp with time zone NOT NULL DEFAULT now();
