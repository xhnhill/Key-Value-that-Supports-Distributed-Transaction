## How to run the UIDBClient.go file


The GUI is written using the **fyne library**, if you do not have the fyne library locally please run the following command to install it

```go
go get -u fyne.io/fyne/v2
```



If you encounter any of the following errors during your run, please install the compiler and restart the run via the link below.


```
error while importing fyne.io/fyne/v2/app: build constraints exclude all Go files in C:\Users\liudi\go\pkg\mod\github.com\go-gl\gl@v0.0.0-20211210172815-726fda9656d6\v3.2-c
```

[TDM-GCC](https://jmeubank.github.io/tdm-gcc/)


Of course if you are operating on a **windows** system, you can run the UIDBClient.exe file **directly** without having to follow the above process to install the dependencies.

## A description of the UI interface

You need to enter the IP address of the node that the current client wants to link to in the first text box and the port of the node in the second text box.

Afterwards, click on the Connect button and you will be prompted in the window whether the connection is successful or unsuccessful.

**Please note that** completing Connect is the basis for everything that follows and you can connect a different node at any time by changing the IP and port values entered.

The third text box is used to enter the key value you want to look up in the key-value database.
The fourth and fifth text boxes are used to enter the key value you want to rewrite and its corresponding value.

**Please note that** these two operations can be performed separately or together. If you want to query multiple sets of key values or change multiple sets of key, value pairs at the same time, please separate the different sets with commas.

Once you have entered the function you wish to perform, click on the Perform Transaction button to do so.

The Fixed Read button is a test to perform a set of fixed queries.