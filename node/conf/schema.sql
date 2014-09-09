CREATE TABLE programs (
	id INT UNSIGNED NOT NULL PRIMARY KEY,
	program VARCHAR(255) NOT NULL,
	pattern VARCHAR(255),
	UNIQUE KEY (program)
) ENGINE=InnoDB;

INSERT INTO programs (id, program) VALUES (1, "none");

CREATE TABLE classes (
	id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
	class VARCHAR(255) NOT NULL,
	parent_id SMALLINT UNSIGNED NOT NULL DEFAULT 0,
	UNIQUE KEY (class)
) ENGINE=InnoDB;

INSERT INTO classes (id, class, parent_id) VALUES(0, "ANY", 0);
INSERT INTO classes (id, class, parent_id) VALUES(1, "NONE", 0);
INSERT INTO classes (id, class, parent_id) VALUES(2, "FIREWALL_ACCESS_DENY", 0);
INSERT INTO classes (id, class, parent_id) VALUES(3, "FIREWALL_CONNECTION_END", 0);
INSERT INTO classes (id, class, parent_id) VALUES(4, "WINDOWS", 0);
INSERT INTO classes (id, class, parent_id) VALUES(7, "URL", 0);
INSERT INTO classes (id, class, parent_id) VALUES(8, "SNORT", 0);
INSERT INTO classes (id, class, parent_id) VALUES(11, "SSH_LOGIN", 0);
INSERT INTO classes (id, class, parent_id) VALUES(12, "SSH_ACCESS_DENY", 0);
INSERT INTO classes (id, class, parent_id) VALUES(13, "SSH_LOGOUT", 0);
INSERT INTO classes (id, class, parent_id) VALUES(14, "BRO_DNS", 0);
INSERT INTO classes (id, class, parent_id) VALUES(15, "BRO_NOTICE", 0);
INSERT INTO classes (id, class, parent_id) VALUES(16, "BRO_SMTP", 0);
INSERT INTO classes (id, class, parent_id) VALUES(17, "BRO_SMTP_ENTITIES", 0);
INSERT INTO classes (id, class, parent_id) VALUES(18, "BRO_SSL", 0);
INSERT INTO classes (id, class, parent_id) VALUES(19, "BRO_HTTP", 0);
INSERT INTO classes (id, class, parent_id) VALUES(20, "BRO_CONN", 0);
INSERT INTO classes (id, class, parent_id) VALUES(21, "FORTINET_URL", 0);
INSERT INTO classes (id, class, parent_id) VALUES(22, "FORTINET_TRAFFIC", 0);
INSERT INTO classes (id, class, parent_id) VALUES(23, "CHECKPOINT", 0);
INSERT INTO classes (id, class, parent_id) VALUES(24, "PALO_ALTO_URL", 0);
INSERT INTO classes (id, class, parent_id) VALUES(25, "PALO_ALTO_TRAFFIC", 0);
INSERT INTO classes (id, class, parent_id) VALUES(26, "OSSEC", 0);
INSERT INTO classes (id, class, parent_id) VALUES(27, "BARRACUDA_SCAN", 0);
INSERT INTO classes (id, class, parent_id) VALUES(28, "BARRACUDA_RECV", 0);
INSERT INTO classes (id, class, parent_id) VALUES(29, "BARRACUDA_SEND", 0);
INSERT INTO classes (id, class, parent_id) VALUES(30, "EXCHANGE", 0);
INSERT INTO classes (id, class, parent_id) VALUES(31, "LOG2TIMELINE", 0);
INSERT INTO classes (id, class, parent_id) VALUES(32, "CEF", 0);
INSERT INTO classes (id, class, parent_id) VALUES(33, "WEB_CONTENT_FILTER", 0);
INSERT INTO classes (id, class, parent_id) VALUES(34, "NETFLOW", 0);
INSERT INTO classes (id, class, parent_id) VALUES(35, "OSSEC_ALERTS", 0);
INSERT INTO classes (id, class, parent_id) VALUES(36, "VPN", 0);
INSERT INTO classes (id, class, parent_id) VALUES(37, "NAT", 0);
INSERT INTO classes (id, class, parent_id) VALUES(38, "FTP", 0);
INSERT INTO classes (id, class, parent_id) VALUES(39, "CISCO_WARN", 0);
INSERT INTO classes (id, class, parent_id) VALUES(40, "DHCP", 0);
INSERT INTO classes (id, class, parent_id) VALUES(41, "BRO_FILE", 0);
INSERT INTO classes (id, class, parent_id) VALUES(42, "FIREEYE", 0);
INSERT INTO classes (id, class, parent_id) VALUES(43, "BRO_FTP", 0);
INSERT INTO classes (id, class, parent_id) VALUES(44, "BRO_WEIRD", 0);
INSERT INTO classes (id, class, parent_id) VALUES(45, "BRO_TUNNEL", 0);
INSERT INTO classes (id, class, parent_id) VALUES(46, "BRO_SOFTWARE", 0);
INSERT INTO classes (id, class, parent_id) VALUES(47, "BRO_SSH", 0);
INSERT INTO classes (id, class, parent_id) VALUES(48, "BRO_SYSLOG", 0);
INSERT INTO classes (id, class, parent_id) VALUES(49, "BRO_IRC", 0);
INSERT INTO classes (id, class, parent_id) VALUES(50, "BRO_KNOWN_CERTS", 0);
INSERT INTO classes (id, class, parent_id) VALUES(51, "BRO_KNOWN_HOSTS", 0);
INSERT INTO classes (id, class, parent_id) VALUES(52, "BRO_KNOWN_SERVICES", 0);
INSERT INTO classes (id, class, parent_id) VALUES(53, "BRO_CAPTURE_LOSS", 0);
INSERT INTO classes (id, class, parent_id) VALUES(54, "BRO_FILES", 0);

INSERT INTO classes (id, class) VALUES(98, "ELSA_UNPARSED");
INSERT INTO classes (id, class) VALUES(99, "ELSA_OPS");

CREATE TABLE class_program_map (
	class_id SMALLINT UNSIGNED NOT NULL,
	program_id INT UNSIGNED NOT NULL,
	PRIMARY KEY (class_id, program_id),
	FOREIGN KEY (class_id) REFERENCES classes (id) ON UPDATE CASCADE ON DELETE CASCADE,
	FOREIGN KEY (program_id) REFERENCES programs (id) ON UPDATE CASCADE ON DELETE CASCADE	
) ENGINE=InnoDB;

CREATE TABLE fields (
	id SMALLINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
	field VARCHAR(255) NOT NULL,
	field_type ENUM("string", "int") NOT NULL,
	pattern_type ENUM("NONE", "QSTRING", "ESTRING", "STRING", "DOUBLE", "NUMBER", "IPv4", "PCRE-IPv4") NOT NULL,
	input_validation VARCHAR(255),
	UNIQUE KEY (field, field_type)
) ENGINE=InnoDB;

INSERT INTO fields (field, field_type, pattern_type) VALUES ("timestamp", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("minute", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("hour", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("day", "int", "NONE");

INSERT INTO fields (field, field_type, pattern_type) VALUES ("host_id", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("program_id", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("class_id", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("host", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("program", "int", "number");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("class", "int", "number");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("msg", "string", "NONE");

INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("ip", "int", "PCRE-IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("proto", "int", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("o_int", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("srcip", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("srcport", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("i_int", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("dstip", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("dstport", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("srcip_nat", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("access_group", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("conn_duration", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("conn_bytes", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("eventid", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("source", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("user", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("field0", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("type", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("hostname", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("category", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("site", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("method", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("uri", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("referer", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("user_agent", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("domains", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("status_code", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("content_length", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("country_code", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_sid", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_msg", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_classification", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_priority", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("authmethod", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("device", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("service", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("port", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("answer", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("notice_type", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("notice_msg", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("server", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("from", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("to", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("subject", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("last_reply", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("path", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("filename", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("content_len", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("mime_type", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("md5", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("extraction_file", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("excerpt", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("expiration", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("group", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("status", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("number", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("interface", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("origin", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("action", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("message_info", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("rule", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("country", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("src_zone", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("dst_zone", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("action_code", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("reason_code", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("reason_extra", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("response", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("hub_server", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("mailbox_server", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("time_taken", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("domain", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("share_name", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("share_path", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("share_target", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("macb", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sourcetype", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("desc", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("notes", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("version", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("vendor", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("product", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_id", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("name", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("severity", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("extension", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("asn", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("city", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("latitude", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("longitude", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("pkts_in", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("pkts_out", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("bytes_in", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("bytes_out", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("line_number", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("pid", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("priority", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("file", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("mac_address", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("cncip", "int", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("cncport", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("virusname", "string", "qstring");

INSERT INTO fields (field, field_type, pattern_type) VALUES ("username", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("password", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("command", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("arg", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("version_major", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("version_minor", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("direction", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("client", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("bro_syslog_facility", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("bro_syslog_severity", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("bro_syslog_message", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("common_name", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("organizational_unit", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("organization", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("email_address", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("gaps", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("acks", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("percent_loss", "string", "QSTRING");

INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("txhosts", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("rxhosts", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("seen_bytes", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("missing_bytes", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sha1", "string", "QSTRING");

INSERT INTO fields (field, field_type, pattern_type) VALUES ("protocol", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sub_msg", "string", "QSTRING");

CREATE TABLE fields_classes_map (
	field_id SMALLINT UNSIGNED NOT NULL,
	class_id SMALLINT UNSIGNED NOT NULL,
	field_order TINYINT UNSIGNED NOT NULL DEFAULT 0,
	PRIMARY KEY (field_id, class_id),
	UNIQUE KEY (class_id, field_order),
	FOREIGN KEY (field_id) REFERENCES fields (id) ON UPDATE CASCADE ON DELETE CASCADE,
	FOREIGN KEY (class_id) REFERENCES classes (id) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB;
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES (0, (SELECT id FROM fields WHERE field="host"), 1);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES (0, (SELECT id FROM fields WHERE field="program"), 2);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES (0, (SELECT id FROM fields WHERE field="class"), 3);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="proto"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="o_int"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="srcport"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="i_int"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="dstip"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="dstport"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="access_group"), 13);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="proto"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="o_int"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="srcport"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="i_int"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="dstip"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="dstport"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="conn_duration"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="conn_bytes"), 10);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="eventid"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="source"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="user"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="domain"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="share_name"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="share_path"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="share_target"), 16);

/*INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="user"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="field0"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="type"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="hostname"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="category"), 16);*/

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="dstip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="status_code"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="content_length"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="country_code"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="time_taken"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="method"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="site"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="uri"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="referer"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="user_agent"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="domains"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_sid"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_msg"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_classification"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="interface"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_priority"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="proto"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="srcip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="srcport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="dstip"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="dstport"), 10);


INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="authmethod"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="user"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="device"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="port"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="service"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="authmethod"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="user"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="device"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="port"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="service"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGOUT"), (SELECT id FROM fields WHERE field="user"), 11);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_DNS"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_DNS"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_DNS"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_DNS"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_DNS"), (SELECT id FROM fields WHERE field="proto"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_DNS"), (SELECT id FROM fields WHERE field="hostname"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_DNS"), (SELECT id FROM fields WHERE field="answer"), 12);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="mime_type"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="desc"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="protocol"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="notice_type"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="notice_msg"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_NOTICE"), (SELECT id FROM fields WHERE field="sub_msg"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILE"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILE"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILE"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILE"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILE"), (SELECT id FROM fields WHERE field="md5"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILE"), (SELECT id FROM fields WHERE field="site"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILE"), (SELECT id FROM fields WHERE field="uri"), 13);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="txhosts"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="rxhosts"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="seen_bytes"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="missing_bytes"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="source"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="mime_type"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="filename"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="conn_duration"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="md5"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FILES"), (SELECT id FROM fields WHERE field="sha1"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="server"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="from"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="to"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="subject"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="last_reply"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP"), (SELECT id FROM fields WHERE field="path"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="filename"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="content_len"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="mime_type"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="md5"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="extraction_file"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SMTP_ENTITIES"), (SELECT id FROM fields WHERE field="excerpt"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSL"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSL"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSL"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSL"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSL"), (SELECT id FROM fields WHERE field="hostname"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSL"), (SELECT id FROM fields WHERE field="subject"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSL"), (SELECT id FROM fields WHERE field="expiration"), 9);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="status_code"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="content_length"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="method"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="site"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="uri"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="referer"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_HTTP"), (SELECT id FROM fields WHERE field="user_agent"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="proto"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="bytes_in"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="service"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="conn_duration"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="bytes_out"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="pkts_out"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CONN"), (SELECT id FROM fields WHERE field="pkts_in"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="user"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="group"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="service"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="site"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="status"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_URL"), (SELECT id FROM fields WHERE field="uri"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_TRAFFIC"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_TRAFFIC"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_TRAFFIC"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_TRAFFIC"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_TRAFFIC"), (SELECT id FROM fields WHERE field="proto"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FORTINET_TRAFFIC"), (SELECT id FROM fields WHERE field="conn_duration"), 10);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="number"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="proto"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="interface"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="origin"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="type"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="action"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CHECKPOINT"), (SELECT id FROM fields WHERE field="service"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="dstip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="content_length"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="rule"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="user"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="category"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="site"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="uri"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_URL"), (SELECT id FROM fields WHERE field="country"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="dstip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="srcport"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="proto"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="conn_bytes"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="src_zone"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="dst_zone"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="i_int"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="o_int"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="country"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="PALO_ALTO_TRAFFIC"), (SELECT id FROM fields WHERE field="category"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SCAN"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SCAN"), (SELECT id FROM fields WHERE field="action_code"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SCAN"), (SELECT id FROM fields WHERE field="reason_code"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SCAN"), (SELECT id FROM fields WHERE field="from"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SCAN"), (SELECT id FROM fields WHERE field="to"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SCAN"), (SELECT id FROM fields WHERE field="reason_extra"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SCAN"), (SELECT id FROM fields WHERE field="subject"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_RECV"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_RECV"), (SELECT id FROM fields WHERE field="action_code"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_RECV"), (SELECT id FROM fields WHERE field="reason_code"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_RECV"), (SELECT id FROM fields WHERE field="from"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_RECV"), (SELECT id FROM fields WHERE field="to"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_RECV"), (SELECT id FROM fields WHERE field="reason_extra"), 13);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SEND"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SEND"), (SELECT id FROM fields WHERE field="action_code"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BARRACUDA_SEND"), (SELECT id FROM fields WHERE field="response"), 11);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="EXCHANGE"), (SELECT id FROM fields WHERE field="hub_server"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="EXCHANGE"), (SELECT id FROM fields WHERE field="mailbox_server"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="EXCHANGE"), (SELECT id FROM fields WHERE field="from"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="EXCHANGE"), (SELECT id FROM fields WHERE field="to"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="EXCHANGE"), (SELECT id FROM fields WHERE field="subject"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="LOG2TIMELINE"), (SELECT id FROM fields WHERE field="macb"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="LOG2TIMELINE"), (SELECT id FROM fields WHERE field="sourcetype"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="LOG2TIMELINE"), (SELECT id FROM fields WHERE field="user"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="LOG2TIMELINE"), (SELECT id FROM fields WHERE field="hostname"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="LOG2TIMELINE"), (SELECT id FROM fields WHERE field="desc"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="LOG2TIMELINE"), (SELECT id FROM fields WHERE field="notes"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CEF"), (SELECT id FROM fields WHERE field="severity"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CEF"), (SELECT id FROM fields WHERE field="vendor"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CEF"), (SELECT id FROM fields WHERE field="product"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CEF"), (SELECT id FROM fields WHERE field="version"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CEF"), (SELECT id FROM fields WHERE field="sig_id"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CEF"), (SELECT id FROM fields WHERE field="name"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CEF"), (SELECT id FROM fields WHERE field="extension"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="dstip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="status_code"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="user"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="site"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="uri"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="category"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="user_agent"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WEB_CONTENT_FILTER"), (SELECT id FROM fields WHERE field="action"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="proto"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="srcport"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="dstip"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="dstport"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="conn_bytes"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="asn"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="country"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="city"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="latitude"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="longitude"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NETFLOW"), (SELECT id FROM fields WHERE field="desc"), 16);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="OSSEC_ALERTS"), (SELECT id FROM fields WHERE field="sig_priority"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="OSSEC_ALERTS"), (SELECT id FROM fields WHERE field="sig_sid"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="OSSEC_ALERTS"), (SELECT id FROM fields WHERE field="sig_msg"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="OSSEC_ALERTS"), (SELECT id FROM fields WHERE field="hostname"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="OSSEC_ALERTS"), (SELECT id FROM fields WHERE field="user"), 13);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="VPN"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="VPN"), (SELECT id FROM fields WHERE field="group"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="VPN"), (SELECT id FROM fields WHERE field="user"), 12);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="ELSA_OPS"), (SELECT id FROM fields WHERE field="line_number"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="ELSA_OPS"), (SELECT id FROM fields WHERE field="pid"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="ELSA_OPS"), (SELECT id FROM fields WHERE field="priority"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="ELSA_OPS"), (SELECT id FROM fields WHERE field="file"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="ELSA_OPS"), (SELECT id FROM fields WHERE field="method"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="ELSA_OPS"), (SELECT id FROM fields WHERE field="hostname"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="proto"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="o_int"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="srcport"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="i_int"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="dstip"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="dstport"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="NAT"), (SELECT id FROM fields WHERE field="srcip_nat"), 10);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="i_int"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="o_int"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="user"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="action"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FTP"), (SELECT id FROM fields WHERE field="filename"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CISCO_WARN"), (SELECT id FROM fields WHERE field="proto"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CISCO_WARN"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CISCO_WARN"), (SELECT id FROM fields WHERE field="srcport"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CISCO_WARN"), (SELECT id FROM fields WHERE field="dstip"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CISCO_WARN"), (SELECT id FROM fields WHERE field="dstport"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CISCO_WARN"), (SELECT id FROM fields WHERE field="i_int"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="CISCO_WARN"), (SELECT id FROM fields WHERE field="o_int"), 12);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="DHCP"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="DHCP"), (SELECT id FROM fields WHERE field="mac_address"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="DHCP"), (SELECT id FROM fields WHERE field="domain"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="DHCP"), (SELECT id FROM fields WHERE field="hostname"), 13);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="cncip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="dstip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="time_taken"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="cncport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="srcip"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="dstport"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="sig_classification"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="hostname"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="virusname"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREEYE"), (SELECT id FROM fields WHERE field="sourcetype"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="eventid"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="username"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="password"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="command"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_FTP"), (SELECT id FROM fields WHERE field="arg"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_WEIRD"), (SELECT id FROM fields WHERE field="eventid"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_WEIRD"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_WEIRD"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_WEIRD"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_WEIRD"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_WEIRD"), (SELECT id FROM fields WHERE field="name"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_TUNNEL"), (SELECT id FROM fields WHERE field="eventid"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_TUNNEL"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_TUNNEL"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_TUNNEL"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_TUNNEL"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_TUNNEL"), (SELECT id FROM fields WHERE field="name"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_TUNNEL"), (SELECT id FROM fields WHERE field="desc"), 13);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SOFTWARE"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SOFTWARE"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SOFTWARE"), (SELECT id FROM fields WHERE field="type"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SOFTWARE"), (SELECT id FROM fields WHERE field="name"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SOFTWARE"), (SELECT id FROM fields WHERE field="version_major"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SOFTWARE"), (SELECT id FROM fields WHERE field="version_minor"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SOFTWARE"), (SELECT id FROM fields WHERE field="product"), 13);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="eventid"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="status"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="direction"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="client"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="server"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SSH"), (SELECT id FROM fields WHERE field="conn_bytes"), 9);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="eventid"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="proto"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="bro_syslog_facility"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="bro_syslog_severity"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_SYSLOG"), (SELECT id FROM fields WHERE field="bro_syslog_message"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_IRC"), (SELECT id FROM fields WHERE field="eventid"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_IRC"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_IRC"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_IRC"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_IRC"), (SELECT id FROM fields WHERE field="dstport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_IRC"), (SELECT id FROM fields WHERE field="desc"), 12);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_CERTS"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_CERTS"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_CERTS"), (SELECT id FROM fields WHERE field="common_name"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_CERTS"), (SELECT id FROM fields WHERE field="organizational_unit"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_CERTS"), (SELECT id FROM fields WHERE field="organization"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_CERTS"), (SELECT id FROM fields WHERE field="email_address"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_HOSTS"), (SELECT id FROM fields WHERE field="srcip"), 5);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_SERVICES"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_SERVICES"), (SELECT id FROM fields WHERE field="srcport"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_SERVICES"), (SELECT id FROM fields WHERE field="proto"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_KNOWN_SERVICES"), (SELECT id FROM fields WHERE field="service"), 11);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CAPTURE_LOSS"), (SELECT id FROM fields WHERE field="interface"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CAPTURE_LOSS"), (SELECT id FROM fields WHERE field="gaps"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CAPTURE_LOSS"), (SELECT id FROM fields WHERE field="acks"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="BRO_CAPTURE_LOSS"), (SELECT id FROM fields WHERE field="percent_loss"), 12);


CREATE TABLE table_types (
	id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
	table_type VARCHAR(255) NOT NULL
) ENGINE=InnoDB;
INSERT INTO table_types (id, table_type) VALUES (1, "index");
INSERT INTO table_types (id, table_type) VALUES (2, "archive");
INSERT INTO table_types (id, table_type) VALUES (3, "import");

CREATE TABLE tables (
	id SMALLINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
	table_name VARCHAR(255) NOT NULL,
	start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	end TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
	min_id BIGINT UNSIGNED NOT NULL DEFAULT 1,
	max_id BIGINT UNSIGNED NOT NULL DEFAULT 1,
	table_type_id TINYINT UNSIGNED NOT NULL,
	table_locked_by INT UNSIGNED,
	FOREIGN KEY (table_type_id) REFERENCES table_types (id),
	UNIQUE KEY (table_name),
	KEY(min_id),
	KEY(max_id),
	UNIQUE KEY (min_id, table_type_id),
	UNIQUE KEY (max_id, table_type_id)
) ENGINE=InnoDB;

CREATE TABLE indexes (
	id SMALLINT UNSIGNED NOT NULL,
	first_id BIGINT UNSIGNED NOT NULL,
	last_id BIGINT UNSIGNED NOT NULL,
	start INT UNSIGNED NOT NULL,
	end INT UNSIGNED NOT NULL,
	table_id SMALLINT UNSIGNED NOT NULL,
	type ENUM("temporary", "permanent", "unavailable", "realtime") NOT NULL DEFAULT "temporary",
	locked_by INT UNSIGNED,
	index_schema TEXT,
	updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (id, type),
	UNIQUE KEY (first_id, last_id),
	KEY(start),
	KEY(end),
	KEY(type),
	KEY(locked_by),
	FOREIGN KEY (table_id) REFERENCES tables (id) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW v_directory AS
SELECT indexes.id, tables.start, tables.end, min_id, max_id, first_id, last_id, table_name,
UNIX_TIMESTAMP(tables.start) AS table_start_int, UNIX_TIMESTAMP(tables.end) AS table_end_int, 
table_types.table_type, tables.id AS table_id,
type, locked_by, table_locked_by,
FROM_UNIXTIME(indexes.start) AS index_start, FROM_UNIXTIME(indexes.end) AS index_end,
indexes.start AS index_start_int, indexes.end AS index_end_int
FROM tables
JOIN table_types ON (tables.table_type_id=table_types.id)
LEFT JOIN indexes ON (tables.id=indexes.table_id);

CREATE TABLE `syslogs_template` (
  `id` bigint unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `timestamp` INT UNSIGNED NOT NULL DEFAULT 0,
  `host_id` INT UNSIGNED NOT NULL DEFAULT '1',
  `program_id` INT UNSIGNED NOT NULL DEFAULT '1',
  `class_id` SMALLINT unsigned NOT NULL DEFAULT '1',
  msg TEXT,
  i0 INT UNSIGNED,
  i1 INT UNSIGNED,
  i2 INT UNSIGNED,
  i3 INT UNSIGNED,
  i4 INT UNSIGNED,
  i5 INT UNSIGNED,
  s0 VARCHAR(255),
  s1 VARCHAR(255),
  s2 VARCHAR(255),
  s3 VARCHAR(255),
  s4 VARCHAR(255),
  s5 VARCHAR(255)
) ENGINE=MyISAM;

CREATE TABLE `init` LIKE `syslogs_template`;
INSERT INTO init (id, timestamp, host_id, program_id, class_id, msg) VALUES (1, 0, 1, 1, 1, "test");

CREATE TABLE stats (
	timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	type ENUM("load", "archive", "index") NOT NULL,
	bytes BIGINT UNSIGNED NOT NULL,
	count BIGINT UNSIGNED NOT NULL,
	time FLOAT UNSIGNED NOT NULL,
	PRIMARY KEY (timestamp, type),
	KEY (type)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS host_stats (
	host_id INT UNSIGNED NOT NULL,
	class_id SMALLINT UNSIGNED NOT NULL,
	count MEDIUMINT UNSIGNED NOT NULL DEFAULT 0,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (timestamp, host_id, class_id)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS buffers (
	id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
	filename VARCHAR(255) NOT NULL,
	pid INT UNSIGNED,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	index_complete BOOLEAN NOT NULL DEFAULT 0,
	archive_complete BOOLEAN NOT NULL DEFAULT 0,
	start INT UNSIGNED,
	end INT UNSIGNED,
	import_id INT UNSIGNED,
	UNIQUE KEY (filename)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS failed_buffers (
	hash CHAR(32) NOT NULL PRIMARY KEY,
	dest VARCHAR(8000) NOT NULL,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	args TEXT,
	pid INT UNSIGNED
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW v_indexes AS
SELECT id, type, FROM_UNIXTIME(start) AS start, FROM_UNIXTIME(end) AS end, last_id-first_id AS records, locked_by
FROM indexes;

CREATE TABLE IF NOT EXISTS livetail (
	qid INT UNSIGNED NOT NULL PRIMARY KEY,
	query BLOB
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS livetail_results (
	qid INT UNSIGNED NOT NULL,
	`id` bigint unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
	`timestamp` INT UNSIGNED NOT NULL DEFAULT 0,
	`host_id` INT UNSIGNED NOT NULL DEFAULT '1',
	`program_id` INT UNSIGNED NOT NULL DEFAULT '1',
	`class_id` SMALLINT unsigned NOT NULL DEFAULT '1',
	msg TEXT,
	i0 INT UNSIGNED,
	i1 INT UNSIGNED,
	i2 INT UNSIGNED,
	i3 INT UNSIGNED,
	i4 INT UNSIGNED,
	i5 INT UNSIGNED,
	s0 VARCHAR(255),
	s1 VARCHAR(255),
	s2 VARCHAR(255),
	s3 VARCHAR(255),
	s4 VARCHAR(255),
	s5 VARCHAR(255),
	FOREIGN KEY (qid) REFERENCES livetail (qid) ON DELETE CASCADE ON UPDATE CASCADE,
	KEY (timestamp)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS imports (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	name VARCHAR(255) NOT NULL,
	description VARCHAR(255) NOT NULL,
	datatype VARCHAR(255) NOT NULL,
	imported TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	first_id BIGINT UNSIGNED,
	last_id BIGINT UNSIGNED,
	KEY(first_id),
	KEY(last_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS uploads (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	client_ip VARCHAR(255) NOT NULL,
	count INT UNSIGNED NOT NULL,
	size BIGINT UNSIGNED NOT NULL,
	batch_time SMALLINT UNSIGNED NOT NULL,
	errors SMALLINT UNSIGNED NOT NULL,
	start INT UNSIGNED NOT NULL,
	end INT UNSIGNED NOT NULL,
	buffers_id INT UNSIGNED NOT NULL
) ENGINE=InnoDB;
