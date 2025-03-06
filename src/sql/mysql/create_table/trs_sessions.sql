CREATE TABLE IF NOT EXISTS trs_sessions (
    id VARCHAR(255) PRIMARY KEY,
    user_id TINYINT NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES trs_users(id)
);