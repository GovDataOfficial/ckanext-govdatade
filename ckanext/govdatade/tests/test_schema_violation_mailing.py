from ckanext.govdatade.commands.report import Report
from ckanext.govdatade.config import config
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader
from mock import Mock

import os
import unittest

class TestSchemaViolationMailing(unittest.TestCase):

    def setUp(self):
        self.report = Report(config)

    def test_cumulate_broken_url_mappings(self):
        data_dict_from_redis = {
            "entries": {
                "http://daten.rlp.de": [{
                    "maintainer_email": "someemail@test.de",
                    "schema": [
                        [
                            "extras.dates.date",
                            "u'2015-02-11 00:00:00' is not a u'date-time'"
                        ],
                        [
                            "extras.geographical_granularity",
                            "u'GERMANFEDERATION' is not one of [u'bund', u'land', u'kommune', u'stadt']"
                        ]
                    ]
                }],
                "http://moo.rlp.de": [{
                    "maintainer_email": "someemail@test.de",
                    "schema": [
                        [
                            "extras.dates.date",
                            "u'2015-02-11 00:00:00' is not a u'date-time'"
                        ],
                        [
                            "extras.geographical_granularity",
                            "u'GERMANFEDERATION' is not one of [u'bund', u'land', u'kommune', u'stadt']"
                        ]
                    ]
                }],
                "http://boo.rlp.de": [{
                    "maintainer_email": "testdon@test.de",
                    "schema": [
                        [
                            "extras.dates.date",
                            "u'2015-02-11 00:00:00' is not a u'date-time'"
                        ]
                    ]
                }]
            },
            "portals": {
                "http://daten.rlp.de": 1,
                "http://moo.rlp.de": 1,
                "http://boo.rlp.de": 1
            }
        }

        expected_cumulated_emails = [
            "someemail@test.de",
            "testdon@test.de"
        ]

        mappings = self.report.cumulate_schema_violation_emails(data_dict_from_redis)
        self.assertEqual(expected_cumulated_emails, mappings)

    def test_get_violation_emails_works_as_expected(self):
        expected_email_address = "test@test.de"

        mail_template_file = 'schema-violations.txt.jinja2'
        mail_template_dir = os.path.dirname(__file__)
        mail_template_dir = os.path.join(
            mail_template_dir,
            '../',
            'mail_assets/templates'
        )
        mail_template_dir = os.path.abspath(mail_template_dir)

        environment = Environment(loader=FileSystemLoader(mail_template_dir))
        mail_template = environment.get_template(mail_template_file)

        report = Report(config)
        report.cumulate_schema_violation_emails = Mock(
            return_value = [expected_email_address]
        )

        expected_violation_emails = [{
            "email_address": expected_email_address,
            "email_content": mail_template.render()
        }]

        self.assertEqual(
            expected_violation_emails,
            report.get_schema_violation_emails({})
        )

    def test_mime_text_message_gets_build_as_expected_and_has_certain_type(self):
        config.set("ckanext.govdata.send.schema.violation.subject", "Test subject schema violation")
        config.set("ckanext.govdata.send.schema.violation.link.from", "no-reply@test.de")

        email_dict = {
            "email_address": "test@test.de",
            "email_content": "Some content text",
        }

        excepted_message = """Content-Type: text/plain; charset="utf-8"
MIME-Version: 1.0
Content-Transfer-Encoding: base64
Subject: Test subject schema violation
From: no-reply@test.de
To: test@test.de

U29tZSBjb250ZW50IHRleHQ=
"""
        message = self.report.get_mime_text_message_for_email(
            email_dict,
            Report.NOTIFICATION_TYPE_SCHEMA_VIOLATIONS
        )

        self.assertEqual(excepted_message, message.as_string())
        self.assertIsInstance(message, MIMEText)

    def test_unexpected_notification_type_raises(self):
        email_dict = {
            "email_address": "test@test.de",
            "email_content": "Some content text",
        }

        with self.assertRaises(AssertionError):
            message = self.report.get_mime_text_message_for_email(
                email_dict,
                "test_type"
            )

    def test_send_notifications_is_disabled(self):
        config.set("ckanext.govdata.send.broken.link.emails", "false")
        config.set("ckanext.govdata.send.schema.violation.emails", False)

        self.assertIsNone(self.report.send_email_notifications({}))

    def test_send_notifications_is_enabled(self):
        config.set("ckanext.govdata.send.broken.link.emails", "false")
        config.set("ckanext.govdata.send.schema.violation.emails", True)

        self.report.get_schema_violation_emails = Mock(return_value = [])
        self.report.send_email_notifications({})

        assertion_msg = 'get_schema_violation_emails was not called and should have been'
        assert self.report.get_schema_violation_emails.called, assertion_msg

    def test_enforced_test_email_is_used_for_schema_violation(self):
        config.set("ckanext.govdata.send.broken.link.emails", False)
        config.set("ckanext.govdata.send.schema.violation.emails", "on")
        config.set("ckanext.govdata.test.email.address", "someone@localhost.org")

        self.report.get_schema_violation_emails = Mock(return_value = [1, 2])

        self.report.send_schema_violation_notifications = Mock()
        self.report.send_email_notifications({})

        args, kwargs = self.report.send_schema_violation_notifications.call_args

        self.assertTrue("forced_email_address" in kwargs)
        self.assertEquals(
            config.get("ckanext.govdata.test.email.address"),
            kwargs["forced_email_address"]
        )