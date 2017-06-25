void calculate(int num) {
	int sum = 0;
    for(int i = 0; i < num; i++)
        for(int j = 0; j < 1000; j++)
            sum = (sum + 1) % 10000;
}