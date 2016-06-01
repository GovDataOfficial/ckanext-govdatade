from ckanext.govdatade.commands.report import Report
from ckanext.govdatade.config import config
from mock import Mock
from email.mime.text import MIMEText

import unittest


class TestLinkCheckerMailing(unittest.TestCase):

    def setUp(self):
        self.report = Report(config)

    def test_broken_urls_are_flatten(self):
        unflattened = {
            "http://test.url?id=1": {
                "date": "2016-02-09",
                "status": "HTTP 404",
                "strikes": 1
            },
            "http://test.url?id=2": {
                "date": "2016-02-09",
                "status": "HTTP 404",
                "strikes": 1
            }
        }

        excepted_flattened = [
            "http://test.url?id=1",
            "http://test.url?id=2"
        ]

        flattened = self.report.flatten_broken_urls(unflattened)
        self.assertEqual(excepted_flattened, flattened)

    def test_cumulate_broken_url_mappings(self):
        data_dict_from_redis = {
            "entries": {
                "http://daten.rlp.de": [{
                    "author_email": "rathaus@gerolstein.de",
                    "id": "7080fe0c-07c6-47f7-8ad9-5884e2aec4a8",
                    "maintainer": "Verbandsgemeinde Gerolstein",
                    "maintainer_email": "someemail@test.de",
                    "metadata_original_portal": "http://daten.rlp.de",
                    "name": "00158f6b-9ab8-44c4-58f3-c64d98a5c8e3",
                    "urls": {
                        "ftp://www.geoportal.rlp.de/mapbender/php/mod_showMetadata.php?languageCode=de&resource=layer&layout=tabs&id=35236": {
                            "date": "2016-02-09",
                            "status": "No connection adapters were found for 'ftp://www.geoportal.rlp.de/mapbender/php/mod_showMetadata.php?languageCode=de&resource=layer&layout=tabs&id=35236'",
                            "strikes": 1
                        },
                        "ftp://www.geoportal.rlp.de:80/portal/karten.html?LAYER[zoom]=1&LAYER[id]=35236": {
                            "date": "2016-02-09",
                            "status": "No connection adapters were found for 'ftp://www.geoportal.rlp.de:80/portal/karten.html?LAYER[zoom]=1&LAYER[id]=35236'",
                            "strikes": 1
                        },
                        "ftps://www.geoportal.rlp.de/mapbender/php/wms.php?layer_id=35236&REQUEST=GetCapabilities&VERSION=1.1.1&SERVICE=WMS": {
                            "date": "2016-02-09",
                            "status": "No connection adapters were found for 'ftps://www.geoportal.rlp.de/mapbender/php/wms.php?layer_id=35236&REQUEST=GetCapabilities&VERSION=1.1.1&SERVICE=WMS'",
                            "strikes": 1
                        },
                        "ftps://www.geoportal.rlp.de:80/portal/karten.html?LAYER[zoom]=1&LAYER[id]=35236": {
                            "date": "2016-02-09",
                            "status": "No connection adapters were found for 'ftps://www.geoportal.rlp.de:80/portal/karten.html?LAYER[zoom]=1&LAYER[id]=35236'",
                            "strikes": 1
                        }
                    }
                }]
            },
            "portals": {
                "http://daten.rlp.de": 1
            }
        }

        expected_mappings = {
            "http://daten.rlp.de": {
                "email": "someemail@test.de",
                "name": "00158f6b-9ab8-44c4-58f3-c64d98a5c8e3",
                "urls": [
                    "ftp://www.geoportal.rlp.de/mapbender/php/mod_showMetadata.php?languageCode=de&resource=layer&layout=tabs&id=35236",
                    "ftps://www.geoportal.rlp.de:80/portal/karten.html?LAYER[zoom]=1&LAYER[id]=35236",
                    "ftp://www.geoportal.rlp.de:80/portal/karten.html?LAYER[zoom]=1&LAYER[id]=35236",
                    "ftps://www.geoportal.rlp.de/mapbender/php/wms.php?layer_id=35236&REQUEST=GetCapabilities&VERSION=1.1.1&SERVICE=WMS"
                ]
            }
        }

        mappings = self.report.cumulate_broken_url_mappings(data_dict_from_redis)
        self.assertEqual(expected_mappings, mappings)

    def test_email_content_is_as_expected(self):
        expected_email_content_list = [{
            'email_content': u'Sehr geehrter Datenbereitsteller,\n\nin denen von Ihnen bereitgestellten Daten f\xfcr http://datenregister.berlin.de sind uns die folgenden nicht aufl\xf6sbaren Verweise aufgefallen:\n\nName des betroffenen Datensatzes: simple_search_wwwberlindebalichtenbergwirtschaftausschreibung\n\nListe der betroffen URL oder URLs:\n  \n+ http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.csv?q=\n  \n+ http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.json?q=\n  \n+ http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.xls?q=\n  \n+ http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php\n  \n+ http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.xml?q=\n  \n+ http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.jrss?q=\n  \n+ http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.rss?q=\n  \n\nUm die Datenqualit\xe4t des Govdata Portals zu erh\xf6hen, bitten wir Sie diese zeitnah zu korrigieren.\n\n\nMit freundlichen Gr\xfc\xdfen,\n\nDas GovData-Team',
            'email_address': 'manuela.maedge@lichtenberg.berlin.de'
        }]

        mappings_mock_return_value = {
            "http://datenregister.berlin.de": {
                "email": "manuela.maedge@lichtenberg.berlin.de",
                "name": "simple_search_wwwberlindebalichtenbergwirtschaftausschreibung",
                "urls": [
                    "http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.csv?q=",
                    "http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.json?q=",
                    "http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.xls?q=",
                    "http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php",
                    "http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.xml?q=",
                    "http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.jrss?q=",
                    "http://www.berlin.de/ba-lichtenberg/wirtschaft/ausschreibung/index.php/index/all.rss?q="
                ]
            }
        }
        self.report.cumulate_broken_url_mappings = Mock(
            return_value=mappings_mock_return_value
        )
        actual_email_content_list = self.report.get_broken_links_emails({})

        self.assertEqual(
            expected_email_content_list,
            actual_email_content_list
        )

    def test_enforced_test_email_is_used_for_broken_link(self):
        config.set("ckanext.govdata.send.broken.link.emails", "true")
        config.set("ckanext.govdata.send.schema.violation.emails", False)
        config.set("ckanext.govdata.test.email.address", "root@localhost")

        self.report.get_broken_links_emails = Mock(return_value = [1, 2])

        self.report.send_broken_link_notifications = Mock()
        self.report.send_email_notifications({})

        args, kwargs = self.report.send_broken_link_notifications.call_args

        self.assertTrue("forced_email_address" in kwargs)
        self.assertEquals(
            config.get("ckanext.govdata.test.email.address"),
            kwargs["forced_email_address"]
        )

    def test_mime_text_message_gets_build_as_expected_and_has_certain_type(self):
        config.set("ckanext.govdata.send.broken.link.subject", "Test Subject")
        config.set("ckanext.govdata.send.broken.link.from", "no-reply@test.de")

        email_dict = {
            "email_address": "test@test.de",
            "email_content": "Some content text",
        }

        excepted_message = """Content-Type: text/plain; charset="utf-8"
MIME-Version: 1.0
Content-Transfer-Encoding: base64
Subject: Test Subject
From: no-reply@test.de
To: test@test.de

U29tZSBjb250ZW50IHRleHQ=
"""
        message = self.report.get_mime_text_message_for_email(
            email_dict,
            Report.NOTIFICATION_TYPE_BROKEN_LINKS
        )

        self.assertEqual(excepted_message, message.as_string())
        self.assertIsInstance(message, MIMEText)
