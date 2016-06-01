from ckanext.govdatade.util import normalize_action_dataset


def test_groups_field():
    groups = [{u'approval_status': u'approved',
               u'capacity': u'public',
               u'created': u'2012-11-01T07:04:45.306038',
               u'description': u'',
               u'id': u'9dafbe15-8fe8-4fae-be78-353aa28391ca',
               u'image_url': u'',
               u'name': u'bildung_wissenschaft',
               u'revision_id': u'73355b6b-20a3-42c9-820f-e399eb65a749',
               u'state': u'active',
               u'title': u'Bildung und Wissenschaft',
               u'type': u'group'},
              {u'approval_status': u'approved',
               u'capacity': u'public',
               u'created': u'2012-11-01T07:04:45.729819',
               u'description': u'',
               u'id': u'0ee8f2f5-bb43-4744-a5f3-3b285cd1fa21',
               u'image_url': u'',
               u'name': u'geo',
               u'revision_id': u'd37ebf3d-86bb-4022-b762-7bcc2ebc8302',
               u'state': u'active',
               u'title': u'Geographie, Geologie und Geobasisdaten',
               u'type': u'group'}]

    dataset = {'groups': groups, 'tags': [], 'extras': []}

    normalize_action_dataset(dataset)
    assert dataset['groups'] == [u'bildung_wissenschaft', u'geo']


def test_tags_field():
    tags = [{u'display_name': u'bauleitplan',
             u'id': u'8dfff9e2-ab24-4b98-9ef8-988dac9bf52a',
             u'name': u'bauleitplan',
             u'revision_timestamp': u'2013-05-10T14:56:33.088324',
             u'state': u'active',
             u'vocabulary_id': None},
            {u'display_name': u'bebauungsplan',
             u'id': u'c3f452ab-d396-40b5-b19d-e4df069f82be',
             u'name': u'bebauungsplan',
             u'revision_timestamp': u'2013-05-10T14:56:33.088324',
             u'state': u'active',
             u'vocabulary_id': None},
            {u'display_name': u'bplan',
             u'id': u'b20f44e0-d704-42eb-aa9a-e30fd5a11b37',
             u'name': u'bplan',
             u'revision_timestamp': u'2013-05-10T14:56:33.088324',
             u'state': u'active',
             u'vocabulary_id': None}]

    dataset = {'groups': [], 'tags': tags, 'extras': []}

    normalize_action_dataset(dataset)
    assert dataset['tags'] == [u'bauleitplan', u'bebauungsplan', u'bplan']


def test_extras_field():
    extras = [{u'id': u'd87c7de7-efeb-4736-be75-bc1be7c616c6',
               u'key': u'sector',
               u'package_id': u'1e7454dc-8ca0-444b-be6e-db8c3a41ff7f',
               u'revision_id': u'8e402ed7-89f9-4d50-8f4f-ee5f9a9ec02f',
               u'revision_timestamp': u'2013-05-10T14:56:33.088324',
               u'state': u'active',
               u'value': u'"oeffentlich"'},
              {u'id': u'763d81b0-fdef-4498-bce7-73d69f619734',
               u'key': u'tag_sources',
               u'package_id': u'1e7454dc-8ca0-444b-be6e-db8c3a41ff7f',
               u'revision_id': u'8e402ed7-89f9-4d50-8f4f-ee5f9a9ec02f',
               u'revision_timestamp': u'2013-05-10T14:56:33.088324',
               u'state': u'active',
               u'value': u'[]'}]

    dataset = {'groups': [], 'tags': [], 'extras': extras}

    normalize_action_dataset(dataset)

    expectation = {u'sector': u'oeffentlich', u'tag_sources': []}
    assert dataset['extras'] == expectation
