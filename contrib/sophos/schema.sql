USE syslog;

INSERT INTO classes (id, class, parent_id) VALUES(10001, "SOPHOS_AV", 0);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SOPHOS_AV"), (SELECT id FROM fields WHERE field="srcip"), 5);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SOPHOS_AV"), (SELECT id FROM fields WHERE field="hostname"), 11);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SOPHOS_AV"), (SELECT id FROM fields WHERE field="user"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SOPHOS_AV"), (SELECT id FROM fields WHERE field="path"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SOPHOS_AV"), (SELECT id FROM fields WHERE field="filename"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SOPHOS_AV"), (SELECT id FROM fields WHERE field="notice_type"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SOPHOS_AV"), (SELECT id FROM fields WHERE field="notice_msg"), 16);
