import xml.etree.ElementTree as ET
import urllib.request as urllib2

APOLLO_TYPES_NAMESPACE = 'http://types.apollo.pitt.edu/v4_0/'
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
        if element.text == 'admin0':
            query["output_options"]['axes'].append('location_admin0')
        elif element.text == 'admin1':
            query["output_options"]['axes'].append('location_admin0')
            query["output_options"]['axes'].append('location_admin1')
        elif element.text == 'admin2':
            query["output_options"]['axes'].append('location_admin0')
            query["output_options"]['axes'].append('location_admin1')
            query["output_options"]['axes'].append('location_admin2')
        elif element.text == 'admin3':
            query["output_options"]['axes'].append('location_admin0')
            query["output_options"]['axes'].append('location_admin1')
            query["output_options"]['axes'].append('location_admin2')
            query["output_options"]['axes'].append('location_admin3')
        elif element.text == 'admin4':
            query["output_options"]['axes'].append('location_admin0')
            query["output_options"]['axes'].append('location_admin1')
            query["output_options"]['axes'].append('location_admin2')
            query["output_options"]['axes'].append('location_admin3')
            query["output_options"]['axes'].append('location_admin4')
        elif element.text == 'admin5':
            query["output_options"]['axes'].append('location_admin0')
            query["output_options"]['axes'].append('location_admin1')
            query["output_options"]['axes'].append('location_admin2')
            query["output_options"]['axes'].append('location_admin3')
            query["output_options"]['axes'].append('location_admin4')
            query["output_options"]['axes'].append('location_admin5')
        elif element.text == 'latLong':
            print ("Error: latLong coordinates are not currently supported")

def get_queries_from_scos(scos_xml_root_node):

    queries = []
    for count_specification in scos_xml_root_node.findall('{' + APOLLO_TYPES_NAMESPACE + '}SimulatorCountOutputSpecification'):

        query = {}
        query["simulator_count_variables"] = {}
        query["file_id"] = -1; # FIX THIS
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

            if field == 'speciesToCount':
                query["simulator_count_variables"]['species'] = {element.text}
                query["output_options"]['axes'].append('species')
            elif field == 'temporalGranularity':
                if element.text != 'entireSimulation':
                    if element.text != 'eachSimulationTimestep':
                        print('Error: unrecognized temporalGranularity. The available options are \'entireSimulation\' and \'eachSimulationTimestep\'')
                        return None
                    query["output_options"]['axes'].append('simulator_time')
            elif field == 'spatialGranularity':
                process_spatial_granularity(query, element)
            elif field == 'infectionState':
                if 'infection_state' in query["simulator_count_variables"]:
                 query["simulator_count_variables"]['infection_state'].add(element.text.upper())
                else:
                    query["simulator_count_variables"]['infection_state'] = {element.text.upper()}
                    query["output_options"]['axes'].append('infection_state')
            elif field == 'diseaseOutcome':
                if 'disease_state' in query["simulator_count_variables"]:
                 query["simulator_count_variables"]['disease_state'].add(element.text.upper())
                else:
                    query["simulator_count_variables"]['disease_state'] = {element.text.upper()}
                    query["output_options"]['axes'].append('disease_state')
            elif field == 'otherVariables':
                process_other_variables(query, element)


        queries.append(query)
    return queries

def get_queries(url):
    tree = ET.ElementTree(file=urllib2.urlopen(url))
    root = tree.getroot()
    queries = get_queries_from_scos(root)
    return queries

if __name__ == '__main__':

    tree = ET.ElementTree(file=urllib2.urlopen('http://localhost/num_infected_by_location.xml'))
    root = tree.getroot()
    queries = get_queries_from_scos(root)
    print(queries)
