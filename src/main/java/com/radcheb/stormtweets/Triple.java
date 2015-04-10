package com.radcheb.stormtweets;

import java.io.Serializable;

public class Triple<T1 extends Serializable, T2 extends Serializable, T3 extends Serializable>
		implements Serializable {
	private static final long serialVersionUID = 42L;

	private T1 car;
	private T2 cdr;
	private T3 caar;

	public Triple(T1 car, T2 cdr, T3 caar){
		this.car = car;
		this.cdr = cdr;
		this.caar = caar;
	}

	public T1 getCar() {
		return car;
	}

	public void setCar(T1 car) {
		this.car = car;
	}

	public T2 getCdr() {
		return cdr;
	}

	public void setCdr(T2 cdr) {
		this.cdr = cdr;
	}

	public T3 getCaar() {
		return caar;
	}

	public void setCaar(T3 caar) {
		this.caar = caar;
	}
}
