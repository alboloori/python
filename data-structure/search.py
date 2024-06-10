#=-=-=-=-=-=-
#Non-recursive Binary Search
def binary_search(ordered_list,term):
    first_index=0
    last_index=len(ordered_list)-1
    mid_index=0
    while first_index<=last_index:
        mid_index=int((first_index+last_index)/2)
        if term==ordered_list[mid_index]:
            return mid_index
        if term<ordered_list[mid_index]:
            last_index=mid_index-1
        else:#term>ordered_list[mid_index]:
            first_index=mid_index+1
    if first_index>last_index:
        return None
a=[2,10, 12 , 18, 22,35,49]
print(binary_search(a,12))
#=-=-=-=-=-=-=-=-=-=
def nearest_mid(input_list, lower_bound_index, upper_bound_index,search_value):
    return lower_bound_index + (( upper_bound_index -lower_bound_index)/
 (input_list[upper_bound_index] -input_list[lower_bound_index])) *(search_value -input_list[lower_bound_index])
def interpolation_search(ordered_list, term):
    size_of_list = len(ordered_list) - 1
    index_of_first_element = 0
    index_of_last_element = size_of_list
    while index_of_first_element <= index_of_last_element:
        mid_point = nearest_mid(ordered_list, index_of_first_element,index_of_last_element, term)
        if mid_point > index_of_last_element or mid_point <index_of_first_element:
            return None
        if ordered_list[mid_point] == term:
            return mid_point
        if term > ordered_list[mid_point]:
            index_of_first_element = mid_point + 1
        else:
            index_of_last_element = mid_point - 1
    if index_of_first_element > index_of_last_element:
        return None
 #=-=-=-=-=-=-=-=-=--=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--==--=-=-=-=-=-=-=-=-=-=--=   
 