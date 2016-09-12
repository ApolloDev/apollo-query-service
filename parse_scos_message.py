import xml.etree.ElementTree as ET
import urllib.request as urllib2

APOLLO_TYPES_NAMESPACE = 'http://types.apollo.pitt.edu/v4_0/'
FILESTORE_TYPES_NAMESPACE = 'http://filestore_service_types.apollo.pitt.edu/v4_0/'
QUERY_SERVICE_NAMESPACE = 'http://query_service_types.apollo.pitt.edu/v4_0/'
XSI_TYPE = '{http://www.w3.org/2001/XMLSchema-instance}type'

def process_other_variables(query, base):
    variable = base.find('{' + APOLLO_TYPES_NAMESPACE + '}variable')
    categories = base.findall('{' + APOLLO_TYPES_NAMESPACE + '}categories')
    range_type = ''
    if variable.text == 'ageRange':
        range_type = 'age_range'
    elif variable.text == 'householdMedianIncome':
        range_type = 'household_median_income'

    query["simulator_count_variables"][range_type] = {}
    query["output_options"]['axes'].append(range_type)

    for category in categories:
        type = category.get(XSI_TYPE)
        if type == 'apollo:MeasuredQuantityRangeCategoryDefinition':
            unit_of_measure = category.find('{' + APOLLO_TYPES_NAMESPACE + '}unitOfMeasure')

            lower_bound = category.find('{' + APOLLO_TYPES_NAMESPACE + '}lowerBound')
            query_lb = process_boundary(lower_bound)

            upper_bound = category.find('{' + APOLLO_TYPES_NAMESPACE + '}upperBound')
            query_ub = process_boundary(upper_bound)

            bound_text = str(query_lb) + ' - ' + str(query_ub)
            query["simulator_count_variables"][range_type][bound_text] = {}
            query["simulator_count_variables"][range_type][bound_text]['range_units'] = unit_of_measure.text
            query["simulator_count_variables"][range_type][bound_text]['range'] = [query_lb, query_ub]

def process_boundary(boundary_element):
    finite_boundry = boundary_element.find('{' + APOLLO_TYPES_NAMESPACE + '}finiteBoundary')
    infinite_boundry = boundary_element.find('{' + APOLLO_TYPES_NAMESPACE + '}infiniteBoundary')
    if finite_boundry is not None:
        lb = int(finite_boundry.text)
    elif infinite_boundry is not None:
        inf_type = infinite_boundry.text
        if inf_type == 'negativeInfinity':
            lb = -float("inf")
        elif inf_type == 'positiveInfinity':
            lb = float("inf")
    return lb

def process_spatial_granularity(query, element):
    if element.text != 'none':
        query["output_options"]['axes'].append('household_location_admin4')
        # if element.text == 'admin0':
        #     query["output_options"]['axes'].append('location_admin0')
        # elif element.text == 'admin1':
        #     query["output_options"]['axes'].append('location_admin0')
        #     query["output_options"]['axes'].append('location_admin1')
        # elif element.text == 'admin2':
        #     query["output_options"]['axes'].append('location_admin0')
        #     query["output_options"]['axes'].append('location_admin1')
        #     query["output_options"]['axes'].append('location_admin2')
        # elif element.text == 'admin3':
        #     query["output_options"]['axes'].append('location_admin0')
        #     query["output_options"]['axes'].append('location_admin1')
        #     query["output_options"]['axes'].append('location_admin2')
        #     query["output_options"]['axes'].append('location_admin3')
        # elif element.text == 'admin4':
        #     query["output_options"]['axes'].append('location_admin0')
        #     query["output_options"]['axes'].append('location_admin1')
        #     query["output_options"]['axes'].append('location_admin2')
        #     query["output_options"]['axes'].append('location_admin3')
        #     query["output_options"]['axes'].append('location_admin4')
        # elif element.text == 'admin5':
        #     query["output_options"]['axes'].append('location_admin0')
        #     query["output_options"]['axes'].append('location_admin1')
        #     query["output_options"]['axes'].append('location_admin2')
        #     query["output_options"]['axes'].append('location_admin3')
        #     query["output_options"]['axes'].append('location_admin4')
        #     query["output_options"]['axes'].append('location_admin5')
        # elif element.text == 'latLong':
        #     print ("Error: latLong coordinates are not currently supported")

def get_file_identification_from_scos(scos_xml_root_node):
    file_identification = scos_xml_root_node.find('{' + QUERY_SERVICE_NAMESPACE + '}outputFileIdentification')

    new_file_id = {}

    for element in file_identification:

        namespace = ''
        field = ''
        if '}' in element.tag:
            field = element.tag.split('}', 1)[1]  # strip all namespaces
            namespace = (element.tag.split('}', 1)[0])[1:]  # strip all namespaces

        if namespace != FILESTORE_TYPES_NAMESPACE:
            print("Error: unsupported Apollo type namespace was used in the XML")
            return None

        new_file_id[field] = element.text

    run_id = scos_xml_root_node.find('{' + QUERY_SERVICE_NAMESPACE + '}runId')
    new_file_id['run_id'] = run_id.text

    return new_file_id


def get_queries_from_scos(scos_xml_root_node):

    queries = []
    for count_specification in scos_xml_root_node.findall('{' + QUERY_SERVICE_NAMESPACE + '}simulatorCountOutputSpecifications'):

        query = {}
        query["simulator_count_variables"] = {}
        query["output_options"] = {}
        query["output_options"]['axes'] = []

        for element in count_specification:

            namespace = ''
            field = ''
            if '}' in element.tag:
                field = element.tag.split('}', 1)[1]  # strip all namespaces
                namespace = (element.tag.split('}', 1)[0])[1:]  # strip all namespaces

            if namespace != APOLLO_TYPES_NAMESPACE:
                print("Error: unsupported Apollo type namespace was used in the XML")
                return None

            # if field == 'speciesToCount':
            #     query["simulator_count_variables"]['species'] = {element.text}
            #     query["output_options"]['axes'].append('species')
            # el
            if field == 'temporalGranularity':
                if element.text != 'entireSimulation':
                    if element.text != 'eachSimulationTimestep':
                        print('Error: unrecognized temporalGranularity. The available options are \'entireSimulation\' and \'eachSimulationTimestep\'')
                        return None
                    query["output_options"]['axes'].append('simulator_time')
            elif field == 'spatialGranularity':
                process_spatial_granularity(query, element)
            elif field == 'infectionStates':
                if 'infection_state' in query["simulator_count_variables"]:
                 query["simulator_count_variables"]['infection_state'].add(element.text.lower())
                else:
                    query["simulator_count_variables"]['infection_state'] = {element.text.lower()}
                    query["output_options"]['axes'].append('infection_state')
            elif field == 'diseaseOutcomes':
                if 'disease_state' in query["simulator_count_variables"]:
                 query["simulator_count_variables"]['disease_state'].add(element.text.lower())
                else:
                    query["simulator_count_variables"]['disease_state'] = {element.text.lower()}
                    query["output_options"]['axes'].append('disease_state')
            elif field == 'otherVariables':
                process_other_variables(query, element)
            elif field == 'simulatorCountOutputSpecificationId':
                query['file_id'] = element.text


        queries.append(query)
        query_container = {}
        query_container['queries'] = queries;
        query_container['output_formats'] = []

        for output_type_element in scos_xml_root_node.findall('{' + QUERY_SERVICE_NAMESPACE + '}outputFormats'):
            output_type = output_type_element.text
            query_container['output_formats'].append(output_type);

    return query_container

def get_queries(url):
    query_container = {}
    tree = ET.ElementTree(file=urllib2.urlopen(url))
    root = tree.getroot()
    queries = get_queries_from_scos(root)
    file_identification = get_file_identification_from_scos(root)
    query_container['query_objects'] = queries
    query_container['file_identification'] = file_identification

    return query_container

if __name__ == '__main__':

    tree = ET.ElementTree(file=urllib2.urlopen('http://localhost/run_message2.xml'))
    root = tree.getroot()
    file_identification = get_file_identification_from_scos(root)
    print(file_identification)
