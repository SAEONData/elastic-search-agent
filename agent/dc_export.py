import xml.etree.ElementTree as ET


def generateXMLDC(root, jsonData):

    header = ET.SubElement(root, 'header')
    if jsonData.get('identifier', '') != '':
        child = ET.SubElement(header, 'identifier')
        child.text = jsonData['identifier']['identifier']
    date = None
    lstDates = jsonData.get('dates', [])
    if len(lstDates) > 0:
        for dte in lstDates:
            date = dte['date']
            if date:
                # Use the first date
                child = ET.SubElement(header, 'datestamp')
                child.text = date
                break

    metadata = ET.SubElement(root, 'metadata')
    oai_dc = ET.SubElement(metadata, 'oai_dc:dc', {
        'xmlns:oai_dc': "http://www.openarchives.org/OAI/2.0/oai_dc/",
        'xmlns:dc': "http://purl.org/dc/elements/1.1/",
        'xmlns:xsi': "http://www.w3.org/2001/XMLSchema-instance",
        'xsi:schemaLocation': """http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd"""})

    lstTitles = jsonData.get('titles', '')
    if lstTitles:
        for ttl in lstTitles:
            title = ttl['title']
            if title:
                # Use the first title
                child = ET.SubElement(oai_dc, 'dc:title')
                child.text = title
                break

    lstCreators = jsonData.get('creators', False)
    if lstCreators:
        for crt in lstCreators:
            creatorName = crt['creatorName']
            if creatorName:
                child = ET.SubElement(oai_dc, 'dc:creator')
                child.text = creatorName

    lstSubjects = jsonData['subjects']
    if lstSubjects:
        for sbj in lstSubjects:
            subject = sbj['subject']
            if subject:
                # Add all subjects
                child = ET.SubElement(oai_dc, 'dc:subject')
                child.text = subject

    year = jsonData.get('publicationYear')
    if year:
        child = ET.SubElement(oai_dc, 'dc:date')
        child.text = year

    if jsonData.get('resourceType', '') != '':
        child = ET.SubElement(oai_dc, 'dc:type', {
            'xsi.type': "dct:DCMIType"})
        child.text = jsonData.get('resourceType')

    # Not sure where to get the type URI
    # child = ET.SubElement(oai_dc, 'dc:identifier', {
    #     'xsi.type': "dct:URI"})
    # child.text = jsonData['identifier']['identifier']

    lstRights = jsonData.get('rights', [])
    if len(lstRights):
        for rgt in lstRights:
            child = ET.SubElement(oai_dc, 'dc:rights')
            child.text = rgt['rights']

    lstDescriptions = jsonData['description']
    if lstDescriptions:
        # descriptionType is empty but shows Abstract on the real xml
        for dsp in lstDescriptions:
            description = ET.SubElement(oai_dc, 'dc:description')
            description.text = dsp['description']

    return
