#=-=-=-=-=-=-=-=-=-==-=-=-=Bubble Sort=-=-=-=-=-=-=-=-=-=-=-=--=
def bubble_sort(unordered_list):
    n=len(unordered_list)-1
    for i in range(n):
        for j in range(n-i):
            if unordered_list[j]>unordered_list[j+1]:
                temp=unordered_list[j]
                unordered_list[j]=unordered_list[j+1]
                unordered_list[j+1]=temp
#=-=-=-=-=-=-=-=-=-=-=-=-=Selection Sort-=-=-=-=-=-=-=-=-=-=-=-=
def selection_sort(unsorted_list):
    for i in range(len(unsorted_list)):
        for j in range(i+1,len(unsorted_list)):
            if unsorted_list[j]<unsorted_list[i]:
                temp=unsorted_list[i]
                unsorted_list[i]=unsorted_list[j]
                unsorted_list[j]=temp
#=-=-=-=-=-=-=-=-=-=-=-=-Insertion Sort-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def insertion_sort(unsorted_list):
    for index in range(1, len(unsorted_list)):
        j = index
        insert_value = unsorted_list[index]
        while j > 0 and unsorted_list[j-1] > insert_value :
            unsorted_list[j] = unsorted_list[j-1]
            j -= 1
        unsorted_list[j] = insert_value
#What is wrong with this quick sort?-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#=-=-=-=-=-=-=-=-=-=-=-=-Quick Sort-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def partition(unsorted_list, first_index, last_index):
        pivot = unsorted_list[first_index]
        pivot_index = first_index
        index_of_last_element = last_index
        less_than_pivot_index = index_of_last_element
        greater_than_pivot_index = first_index + 1
        while True:
            while unsorted_list[greater_than_pivot_index] < pivot and greater_than_pivot_index < last_index:
                greater_than_pivot_index += 1
            while unsorted_list[less_than_pivot_index] > pivot and less_than_pivot_index >= first_index:
                less_than_pivot_index -= 1
def quick_sort(unsorted_list, first, last):
        if last - first <= 0:
            return
        else:
            partition_point = partition(unsorted_list, first, last)
            quick_sort(unsorted_list, first, partition_point-1)
            quick_sort(unsorted_list, partition_point+1, last)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
a=[6,5,3,4,1,2,8,6]
print(a)
insertion_sort(a,0,len(a)-1)
print(a)