import xml.etree.ElementTree as ET


def generateXML(root, jsonData):

    if jsonData.get('identifier', '') != '':
        child = ET.SubElement(root, 'identifier', {
            'identifierType': jsonData['identifier']['identifierType']})
        child.text = jsonData['identifier']['identifier']

    lstCreators = jsonData.get('creators', False)
    if lstCreators:
        creators = ET.SubElement(root, 'creators')
        for crt in lstCreators:
            creator = ET.SubElement(creators, 'creator')

            creatorName = ET.SubElement(creator, 'creatorName')
            creatorName.text = crt['creatorName']
            if crt.get('nameIdentifier', '') != '':
                nameIdentifier = ET.SubElement(creator, 'nameIdentifier', {
                    'nameIdentifierScheme': crt['nameIdentifierScheme'],
                    'schemeURI': crt['schemeURI']})
                nameIdentifier.text = crt['nameIdentifier']
            if crt.get('affiliation', '') != '':
                affiliation = ET.SubElement(creator, 'affiliation')
                affiliation.text = crt['affiliation']

    lstTitles = jsonData.get('titles', '')
    if lstTitles:
        titles = ET.SubElement(root, 'titles')
        for ttl in lstTitles:
            title = ET.SubElement(titles, 'title')
            title.text = ttl['title']
            if ttl.get('titleType', '') != '':
                title.set('titleType', ttl['titleType'])

    if jsonData.get('publisher'):
        publisher = ET.SubElement(root, 'publisher')
        publisher.text = jsonData['publisher']

    if jsonData.get('publicationYear'):
        publicationYear = ET.SubElement(root, 'publicationYear')
        publicationYear.text = jsonData['publicationYear']

    lstSubjects = jsonData['subjects']
    if lstSubjects:
        subjects = ET.SubElement(root, 'subjects')
        for sbj in lstSubjects:
            subject = ET.SubElement(subjects, 'subject')
            subject.text = sbj['subject']
            if sbj.get('schemeURI', '') != '':
                subject.set('schemeURI', sbj['schemeURI'])
            if sbj.get('subjectScheme', '') != '':
                subject.set('subjectScheme', sbj['subjectScheme'])

    lstContributors = jsonData.get('contributors')
    if lstContributors:
        contributors = ET.SubElement(root, 'contributors')
        for ctb in lstContributors:
            contributor = ET.SubElement(contributors, 'contributor')
            if ctb.get('contributorType', '') != '':
                contributor.set('contributorType', ctb['contributorType'])

            contributorName = ET.SubElement(
                contributor, 'contributorName')
            contributorName.text = ctb['contributorName']
            if ctb.get('nameIdentifier', '') != '':
                nameIdentifier = ET.SubElement(
                    contributor, 'nameIdentifier')
                nameIdentifier.text = ctb['nameIdentifier']
                if ctb.get('nameIdentifierScheme', '') != '':
                    nameIdentifier.set(
                        'nameIdentifierScheme',
                        ctb['nameIdentifierScheme'])
                if ctb.get('schemeURI', '') != '':
                    nameIdentifier.set('schemeURI', ctb['schemeURI'])
            if ctb.get('affiliation', '') != '':
                affiliation = ET.SubElement(contributor, 'affiliation')
                affiliation.text = ctb['affiliation']

    lstDates = jsonData.get('dates', [])
    if len(lstDates) > 0:
        dates = ET.SubElement(root, 'dates')
        for dte in lstDates:
            date = ET.SubElement(dates, 'date')
            date.text = dte['date']
            date.set('dateType', dte['dateType'])

    if jsonData.get('language', '') != '':
        language = ET.SubElement(root, 'language')
        language.text = jsonData['language']

    if jsonData.get('resourceType', '') != '':
        resourceType = ET.SubElement(root, 'resourceType')
        # resourceTypeGeneral is empty but shows Software on the real xml
        resourceType.set(
            'resourceTypeGeneral', jsonData['resourceTypeGeneral'])
        resourceType.text = jsonData['resourceType']

    lstAlternateIdentifiers = jsonData.get('alternateIdentifiers', [])
    if len(lstAlternateIdentifiers) > 0:
        alternateIdentifiers = ET.SubElement(root, 'alternateIdentifiers')
        for atf in lstAlternateIdentifiers:
            alternateIdentifier = ET.SubElement(
                alternateIdentifiers, 'alternateIdentifier')
            alternateIdentifier.text = atf['alternateIdentifier']
            if atf.get('alternateIdentifierType', False):
                alternateIdentifier.set(
                    'alternateIdentifierType',
                    atf['alternateIdentifierType'])

    lstRelatedIdentifier = jsonData.get('relatedIdentifiers', False)
    if lstRelatedIdentifier:
        relatedIdentifiers = ET.SubElement(root, 'relatedIdentifiers')
        for rld in lstRelatedIdentifier:
            relatedIdentifier = ET.SubElement(
                relatedIdentifiers,
                'relatedIdentifier')
            relatedIdentifier.text = rld['relatedIdentifier']
            relatedIdentifier.set(
                'relatedIdentifierType', rld['relatedIdentifierType'])
            relatedIdentifier.set('relationType', rld['relationType'])
            relatedIdentifier.set(
                'relatedMetadataScheme', rld['relatedMetadataScheme'])
            relatedIdentifier.set('schemeURI', rld['schemeURI'])

    lstSizes = jsonData.get('sizes', False)
    if lstSizes:
        sizes = ET.SubElement(root, 'sizes')
        for sze in lstSizes:
            size = ET.SubElement(sizes, 'size')
            size.text = sze

    lstFormats = jsonData.get('formats', [])
    if len(lstFormats) > 0:
        formats = ET.SubElement(root, 'formats')
        for fmt in lstFormats:
            format = ET.SubElement(formats, 'format')
            format.text = str(fmt)

    if jsonData.get('version', '') != '':
        version = ET.SubElement(root, 'version')
        version.text = jsonData['version']

    lstRights = jsonData.get('rights', [])
    if len(lstRights):
        rightsList = ET.SubElement(root, 'rightsList')
        for rgt in lstRights:
            rights = ET.SubElement(rightsList, 'rights')
            rights.text = rgt['rights']
            if rgt.get('rightsURI', '') != '':
                rights.set('rightsURI', rgt['rightsURI'])

    lstDescriptions = jsonData['description']
    if lstDescriptions:
        descriptions = ET.SubElement(root, 'descriptions')
        # descriptionType is empty but shows Abstract on the real xml
        for dsp in lstDescriptions:
            description = ET.SubElement(descriptions, 'description')
            description.text = dsp['description']
            if dsp.get('descriptionType', ''):
                description.set('descriptionType',
                                dsp['descriptionType'])

    lstGeoLocations = jsonData.get('geoLocations', [])
    if len(lstGeoLocations):
        geoLocations = ET.SubElement(root, 'geoLocations')
        for gln in lstGeoLocations:
            geoLocation = ET.SubElement(geoLocations, 'geoLocation')
            # TODO Why is this here??? gln = gln['geoLocation']
            if gln.get('geoLocationPoint', '') != '':
                geoLocationPoint = ET.SubElement(
                    geoLocation, 'geoLocationPoint')
                geoLocationPoint.text = gln['geoLocationPoint']

            if gln.get('geoLocationBox', '') != '':
                geoLocationBox = ET.SubElement(
                    geoLocation, 'geoLocationBox')
                geoLocationBox.text = gln['geoLocationBox']

            if gln.get('geoLocationPlace', '') != '':
                geoLocationPlace = ET.SubElemen(
                    geoLocation, 'geoLocationPlace')
                geoLocationPlace.text = gln['geoLocationPlace']

    return
    # addFields = jsonData['additionalFields']
    # additionalFields = ET.SubElement('additionalFields')
    # #descriptionType is empty but shows Abstract on the real xml
    # for dsp in lstDescriptions:
    #     description = ET.SubElement('description')
    #     descriptionText = ET.SubElement(dsp['description'])
    #     if dsp.get('descriptionType', ''):
    #         description.setAttribute('descriptionType',
    #                                  dsp['descriptionType'])

    # lstResources = jsonData.get('onlineResources', '')
    # if len(lstResources):
    #     resources = ET.SubElement('onlineResources')
    #     for rcs in lstResources:
    #         description = ET.SubElement('description')
    #         descriptionText = ET.SubElement(rcs['description'])
    #         if rcs.get('descriptionType', ''):
    #             description.setAttribute('descriptionType',
    #                                      rcs['descriptionType'])
    return
