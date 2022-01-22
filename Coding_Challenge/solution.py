def single_pair(arr: list, a: int) -> None:
    """
    This function takes an array of integers and required sum as input and find the two positions in the array whose
    value is equal to the value provided in 'a' variable. This function will only return single pair (first one if
    multiple are present)
    :param arr: array of integers
    :param a: required sum of values
    :return: NA
    """
    # create an empty dictionary to store processed elements
    processed_elements = {}
    # enumerate for each element
    for index, ele in enumerate(arr):
        # check if pair (ele, a - ele) exists and print if the difference is already present
        if a - ele in processed_elements:
            print((processed_elements.get(a - ele), index))
            return
        # store index of the current element in the dictionary
        processed_elements[ele] = index


def multiple_pair(arr: list, a: int) -> None:
    """
        This function takes an array of integers and required sum as input and find the two positions in the array whose
        value is equal to the value provided in 'a' variable. This function will return multiple pairs if present
        :param arr: array of integers
        :param a: required sum of values
        :return: NA
        """
    # create an empty dictionary to store processed elements
    processed_elements = {}
    # enumerate for each element
    for index, ele in enumerate(arr):
        # check if pair (ele, a - ele) exists and print the pair if the difference is already present
        if a - ele in processed_elements:
            print((processed_elements.get(a - ele), index))
        # store index of the current element in the processed_elements dictionary
        processed_elements[ele] = index
    return


if __name__ == '__main__':
    arr = [-1, 6, 10, 3, 2, -10, 19]
    a = 9
    print("Getting single pair: ")
    single_pair(arr, a)
    print("Getting multiple pairs: ")
    multiple_pair(arr, a)
