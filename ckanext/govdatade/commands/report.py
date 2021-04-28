#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Module for reporting the quality (ogd schema adherence, dead links)
of data in the CKAN instance.
'''
import logging
import os
import smtplib
from collections import defaultdict
from email.mime.text import MIMEText

from ckan.lib.cli import CkanCommand
from jinja2 import Environment, FileSystemLoader
from pylons import config
from validate_email import validate_email

from ckanext.govdatade.util import amend_portal
from ckanext.govdatade.util import boolize_config_value
from ckanext.govdatade.util import copy_report_asset_files
from ckanext.govdatade.util import copy_report_vendor_files
from ckanext.govdatade.util import generate_general_data
from ckanext.govdatade.util import generate_link_checker_data
from ckanext.govdatade.util import generate_schema_checker_data


class Report(CkanCommand):

    '''Generates metadata quality report based on Redis data.'''

    summary = __doc__.split('\n')[0]

    NOTIFICATION_TYPE_BROKEN_LINKS = 'broken-link-notification'
    NOTIFICATION_TYPE_SCHEMA_VIOLATIONS = 'schema-violation-notification'

    def __init__(self, name):
        super(Report, self).__init__(name)
        self.logger = logging.getLogger('ckanext.govdatade.reports')

    def generate_report(self):
        '''
        Generates the report
        '''
        data = defaultdict(defaultdict)

        generate_general_data(data)
        generate_link_checker_data(data)
        generate_schema_checker_data(data)

        copy_report_asset_files()
        copy_report_vendor_files()

        templates = ['index.html', 'linkchecker.html', 'schemachecker.html']
        templates = map(lambda name: name + '.jinja2', templates)

        for template_file in templates:
            rendered_template = self.render_template(template_file, data)
            self.write_validation_result(rendered_template, template_file)

        self.send_email_notifications(data)

    def send_email_notifications(self, data):
        '''
        Sends the broken link and schema violation emails
        '''
        should_send_broken_link_notifications = boolize_config_value(
            config.get(
                'ckanext.govdata.send.broken.link.emails',
                default=False
            )
        )

        should_send_schema_violation_notifications = boolize_config_value(
            config.get(
                'ckanext.govdata.send.schema.violation.emails',
                default=False
            )
        )

        forced_email_address = config.get(
            'ckanext.govdata.test.email.address',
            default=None
        )

        if should_send_broken_link_notifications:
            broken_link_emails = self.get_broken_links_emails(data)
            if len(broken_link_emails) > 0:
                self.send_broken_link_notifications(
                    broken_link_emails,
                    forced_email_address=forced_email_address
                )
        elif should_send_schema_violation_notifications:
            info_message = "Sending broken link notification emails "
            info_message += "is disabled."
            self.logger.info(info_message)

        if should_send_schema_violation_notifications:
            schema_violation_emails = self.get_schema_violation_emails(data)
            if len(schema_violation_emails) > 0:
                self.send_schema_violation_notifications(
                    schema_violation_emails,
                    forced_email_address=forced_email_address
                )
        elif should_send_broken_link_notifications:
            info_message = "Sending schema violation notification emails "
            info_message += "is disabled."
            self.logger.info(info_message)

        if not should_send_broken_link_notifications and not should_send_schema_violation_notifications:
            info_message = "Sending notification emails is disabled."
            self.logger.info(info_message)

            return None

    def send_schema_violation_notifications(self, emails_dict, forced_email_address=None):
        '''
        Sends the schema viloation notification emails
        '''
        smtp_server = config.get("smtp.server")
        smtp_connection = smtplib.SMTP(smtp_server)

        if forced_email_address:
            info_message = "Schema violation notification email "
            info_message += "address is enforced. Set to %s."
            self.logger.info(info_message, forced_email_address)

        mail_sent_amount = 0

        for email in emails_dict:
            email_to_send_to = email["email_address"]

            if forced_email_address and email_to_send_to:
                email_to_send_to = forced_email_address
                email["email_address"] = email_to_send_to

            if validate_email(email_to_send_to):
                message = self.get_mime_text_message_for_email(
                    email,
                    self.NOTIFICATION_TYPE_SCHEMA_VIOLATIONS
                )
                smtp_connection.sendmail(
                    message['From'],
                    message['To'],
                    message.as_string()
                )
                mail_sent_amount += 1

        info_message = "Sent %s schema violation notification mail(s)."
        self.logger.info(info_message, mail_sent_amount)

        smtp_connection.quit()


    def send_broken_link_notifications(self, emails_dict, forced_email_address=None):
        '''
        Sends the broken link notification emails
        '''
        smtp_server = config.get("smtp.server")
        smtp_connection = smtplib.SMTP(smtp_server)

        if forced_email_address:
            info_message = "Broken link notification email "
            info_message += "address is enforced. Set to %s."
            self.logger.info(info_message, forced_email_address)

        mail_sent_amount = 0

        for email in emails_dict:
            email_to_send_to = email["email_address"]

            if forced_email_address and email_to_send_to:
                email_to_send_to = forced_email_address
                email["email_address"] = email_to_send_to

            if validate_email(email_to_send_to):
                message = self.get_mime_text_message_for_email(
                    email,
                    self.NOTIFICATION_TYPE_BROKEN_LINKS
                )
                smtp_connection.sendmail(
                    message['From'],
                    message['To'],
                    message.as_string()
                )
                mail_sent_amount += 1

        info_message = "Sent %s broken link notification mail(s)."
        self.logger.info(info_message, mail_sent_amount)

        smtp_connection.quit()

    @classmethod
    def get_mime_text_message_for_email(cls, email_dict, notification_type):
        '''
        Builds and returns a MIMEText object from the
        given email dictionary and notification type
        '''
        message = MIMEText(
            email_dict["email_content"].encode('utf-8'),
            'plain',
            'utf-8'
        )

        expected_notification_types = [
            cls.NOTIFICATION_TYPE_SCHEMA_VIOLATIONS,
            cls.NOTIFICATION_TYPE_BROKEN_LINKS,
        ]

        assert_message = "Given notification type has unexpected type"
        assert notification_type in expected_notification_types, assert_message

        if notification_type == cls.NOTIFICATION_TYPE_SCHEMA_VIOLATIONS:
            message["Subject"] = config.get("ckanext.govdata.send.schema.violation.subject")
            message["From"] = config.get("ckanext.govdata.send.schema.violation.link.from")
        elif notification_type == cls.NOTIFICATION_TYPE_BROKEN_LINKS:
            message["Subject"] = config.get("ckanext.govdata.send.broken.link.subject")
            message["From"] = config.get("ckanext.govdata.send.broken.link.from")

        message["To"] = email_dict["email_address"]

        return message

    @classmethod
    def flatten_broken_urls(cls, data):
        '''
        Flattens the broken URLs dictionary to a
        simple list
        '''
        urls = []
        for url in data:
            urls.append(url)

        return urls

    @classmethod
    def cumulate_broken_url_mappings(cls, data):
        '''
        Cumulates the broken URL mappings from the given dictionary
        '''
        broken_url_mappings = {}
        for k in data["portals"]:
            entries = data["entries"][k]
            for entry in entries:
                if entry["maintainer_email"]:
                    broken_url_mappings[k] = {
                        "name": entry["name"],
                        "email": entry["maintainer_email"],
                        "urls": cls.flatten_broken_urls(entry["urls"])
                    }

        return broken_url_mappings


    def get_broken_links_emails(self, data):
        '''
        Returns a list of broken link notification emails
        '''

        broken_url_mappings = self.cumulate_broken_url_mappings(data)

        mail_template_file = 'broken-links.txt.jinja2'
        mail_template_dir = os.path.dirname(__file__)
        mail_template_dir = os.path.join(
            mail_template_dir,
            '../',
            'mail_assets/templates'
        )
        mail_template_dir = os.path.abspath(mail_template_dir)

        environment = Environment(loader=FileSystemLoader(mail_template_dir))
        mail_template = environment.get_template(mail_template_file)

        emails = []
        for portal in broken_url_mappings:
            emails.append({
                "email_address": broken_url_mappings[portal]["email"],
                "email_content": mail_template.render(
                    broken_urls=broken_url_mappings[portal],
                    portal=portal
                )
            })

        return emails

    @classmethod
    def cumulate_schema_violation_emails(cls, data):
        '''
        Cumulates the schema_violation emails from
        the given dictionary
        '''
        cumulated_emails = []
        for k in data["portals"]:
            entries = data["entries"][k]
            for entry in entries:
                if entry["maintainer_email"]:
                    if entry["maintainer_email"] not in cumulated_emails:
                        cumulated_emails.append(
                            entry["maintainer_email"]
                        )

        return cumulated_emails

    def get_schema_violation_emails(self, data):
        '''
        Returns a list of schema violation notification emails
        '''
        cumulated_emails = self.cumulate_schema_violation_emails(data)

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

        emails = []
        for email in cumulated_emails:
            emails.append({
                "email_address": email,
                "email_content": mail_template.render()
            })

        return emails

    @classmethod
    def render_template(cls, template_file, data):
        '''
        Renders the report template
        '''
        template_dir = os.path.dirname(__file__)
        template_dir = os.path.join(
            template_dir,
            '../',
            'report_assets/templates'
        )
        template_dir = os.path.abspath(template_dir)

        environment = Environment(loader=FileSystemLoader(template_dir))
        environment.globals.update(amend_portal=amend_portal)

        data['ckan_api_url'] = config.get('ckan.api.url.portal')
        data['govdata_detail_url'] = config.get(
            'ckanext.govdata.validators.report.detail.url'
        )

        template = environment.get_template(template_file)
        return template.render(data)

    @classmethod
    def write_validation_result(cls, rendered_template, template_file):
        '''
        Writes the report to the filesystem
        '''
        target_template = template_file.rstrip('.jinja2')

        target_dir = config.get('ckanext.govdata.validators.report.dir')

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        target_file = os.path.join(target_dir, target_template)
        target_file = os.path.abspath(target_file)

        file_handler = open(target_file, 'w')
        file_handler.write(rendered_template.encode('UTF-8'))
        file_handler.close()

    def command(self):
        '''
        Entry method for the command
        '''
        super(Report, self)._load_config()
        self.generate_report()
        report_path = os.path.normpath(
            config.get('ckanext.govdata.validators.report.dir')
        )
        info_message = "Wrote validation report to '%s'." % report_path
        self.logger.info(info_message)
