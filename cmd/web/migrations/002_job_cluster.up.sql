-- Existing Job records predate explicit execution contexts.
ALTER TABLE jobs ADD COLUMN cluster text NOT NULL DEFAULT '';
