conversations = [{"name": "id", "type": "varchar(256)"},
                 {"name": "direction", "type": "varchar(256)"},
                 {"name": "subject", "type": "varchar(512)"},
                 {"name": "display_content", "type": "text"},
                 {"name": "html_content", "type": "text"},
                 {"name": "current_html_content", "type": "text"},
                 {"name": "is_read", "type": "boolean"},
                 {"name": "count", "type": "int"},
                 {"name": "meta_thread_attachments", "type": "text"},
                 {"name": "email_id", "type": "int"},
                 {"name": "status", "type": "int"},
                 {"name": "needs_response", "type": "boolean"},
                 {"name": "needs_tracking", "type": "boolean"},
                 {"name": "template_id", "type": "int"},
                 {"name": "scheduled_at", "type": "timestamptz"},
                 {"name": "attachments", "type": "varchar(max)"},
                 {"name": "conversation_time", "type": "timestamptz"},
                 {"name": "shrink_content", "type": "varchar(max)"},
                 {"name": "conversation_meta", "type": "varchar(max)"},
                 {"name": "email_association", "type": "boolean"}]
