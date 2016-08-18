package com.hadoop.log.predict;

public class PredictUtils {

	/**
	 * 获取第index个数的预测值
	 * @param data
	 * @param index
	 * @return
	 */
	public static double getPredictValue(double[] data, int index){
		double[][] B = getB(getAGO(data));
		double[][] Y = getY(data);
		double[] u = getU(B, Y);
		if(u == null || u[0] == 0 )
			return getAveValue(data);
		return getPredictValue(data, u, index);
	}
	
	private static double getAveValue(double[] data){
		double ave = 0.0;
		for (double d : data) {
			ave += d;
		}
		ave /= data.length;
		return ave;
	}
	
	/**
	 * 获取第index个数预测值
	 * @param data 原始数据
	 * @param u [a, b]系数数组
	 * @param index 待预测的第index个数，从1开始
	 * @return
	 */
	private static double getPredictValue(double[] data, double[] u, int index){
		double value = 0.0;
		if(index == 1)
			return data[0];
		value = (data[0] - (u[1] / u[0])) * Math.exp((-1) * u[0] * (index - 1)) + (u[1] / u[0]);
		value -= (data[0] - (u[1] / u[0])) * Math.exp((-1) * u[0] * (index - 2)) + (u[1] / u[0]);
		return value;
	}
	
	/**
	 * 获取一次累加生成数
	 * @param data
	 * @return
	 */
	private static double[] getAGO(double[] data){
		double[] newData = new double[data.length];
		for(int i = 0; i < data.length; i++){
			double sum = 0.0;
			for(int j = 0; j <= i; j++)
				sum += data[j];
			newData[i] = sum;
		}
		return newData;
	}
	
	/**
	 * 获取由一次累加数计算得的数据矩阵
	 * @param AGO 一次累加生成数
	 * @return
	 */
	private static double[][] getB(double[] AGO){
		double[][] B = new double[AGO.length - 1][2];
		for(int i = 0; i < B.length; i++){
			B[i][0] = (-0.5) * (AGO[i] + AGO[i + 1]);
			B[i][1] = 1.0;
		}
		return B;
	}
	
	/**
	 * 获取由原始数据求得的数据矩阵
	 * @param data 原始数据
	 * @return
	 */
	private static double[][] getY(double[] data){
		double[][] B = new double[data.length - 1][1];
		for(int i = 0; i < B.length; i++){
			B[i][0] = data[i + 1];
		}
		return B;
	}
	
	/**
	 * 获取[a, b]系数数组
	 * @param B
	 * @param Y
	 * @return
	 */
	private static double[] getU(double[][] B, double[][] Y){
		double[] u = new double[2];
		double[][] U = null;
		double[][] temp = multiple(transpose(B), B);
		if(getDet(temp) == 0)
			return null;
		U = multiple(multiple(getInverse(temp), transpose(B)), Y);
		u[0] = U[0][0];
		u[1] = U[1][0];
		return u;
	}
	
	/**
	 * 获取代数余子式
	 * @param data
	 * @param x
	 * @param y
	 * @return
	 */
	private static double[][] getCofactor(double[][] data, int x, int y){
		double[][] newData = new double[data.length - 1][data.length - 1];
		
		for(int i = 0; i < data.length; i++){
			if(i == x)
				continue;
			for(int j = 0; j < data.length; j++){
				if(j == y)
					continue;
				int x0 = (i < x) ? i : (i-1);
				int y0 = (j < y) ? j : (j-1);
				newData[x0][y0] = data[i][j];
			}
		}
		
		return newData;
	}
	
	private static double getDet2(double[][] data){
		return data[0][0] * data[1][1] - data[0][1] * data[1][0];
	}
	
	/**
	 * 求行列式的值
	 * @param data
	 * @return
	 */
	private static double getDet(double[][] data){
		if(data.length == 1)
			return data[0][0];
		if(data.length == 2)
			return getDet2(data);
		double det = 0.0;
		for(int i = 0; i < data.length; i++){
			if(i % 2 == 0)
				det += data[0][i] * getDet(getCofactor(data, 0, i));
			else
				det -= data[0][i] * getDet(getCofactor(data, 0, i));
		}
		return det;
	}
	
	
	/**
	 * 矩阵转置
	 * @param data
	 */
	private static double[][] transpose(double[][] data){
		double[][] newData = new double[data[0].length][data.length];
		for(int i = 0; i < data.length; i++){
			for(int j = 0; j < data[0].length; j++){
				newData[j][i] = data[i][j];
			}
		}
		return newData;
	}
	
	/**
	 * 求得矩阵的逆
	 * @param data
	 * @return
	 */
	private static double[][] getInverse(double[][] data){
		double[][] newData = new double[data.length][data.length];
		for(int i = 0; i < data.length; i++){
			for(int j = 0; j < data.length; j++){
				newData[i][j] = (((i + j) % 2) == 0 ? 1 : -1) * getDet(getCofactor(data, i, j));
			}
		}
		newData = transpose(newData);
		
		double det = getDet(data);
		for(int i = 0; i < data.length; i++){
			for(int j = 0; j < data.length; j++){
				newData[i][j] /= det;
			}
		}
		
		return newData;
	}
	
	/**
	 * 求矩阵的乘积
	 * @param data1
	 * @param data2
	 * @return
	 */
	private static double[][] multiple(double[][] data1, double[][] data2){
		double[][] newData = new double[data1.length][data2[0].length];
		for(int i = 0; i < data1.length; i++){
			for(int j = 0; j < data2[0].length; j++){
				double sum = 0.0;
				for(int k =0; k < data1[0].length; k++)
					sum += data1[i][k] * data2[k][j];
				newData[i][j] = sum;
			}
		}
		return newData;
	}

	
}
