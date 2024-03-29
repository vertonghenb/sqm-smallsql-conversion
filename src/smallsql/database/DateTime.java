package smallsql.database;
import java.sql.*;
import java.text.DateFormatSymbols;
import java.util.Calendar;
import java.util.TimeZone;
import smallsql.database.language.Language;
public final class DateTime implements Mutable{
	long time;
	private int dataType = SQLTokenizer.TIMESTAMP;
	static final int[] MONTH_DAYS = {0,31,59,90,120,151,181,212,243,273,304,334}; 
	private static final String[] SHORT_MONTHS = new DateFormatSymbols().getShortMonths();
	DateTime(long time, int dataType){
		switch(dataType){
        case SQLTokenizer.SMALLDATETIME:
            int seconds = (int)(time % 60000);
            if(seconds < 0){
                seconds += 60000;
            }
            time -= seconds;
            break;
		case SQLTokenizer.TIME:
			time %= 86400000;
			break;
		case SQLTokenizer.DATE:
			int millis = (int)(time % 86400000);
			if(millis < 0)
				millis += 86400000;
			time -= millis;
			break;
		}
		this.time = time;
		this.dataType = dataType;
	}
	static long calcMillis(Details details){
		return calcMillis(details.year, details.month, details.day, details.hour, details.minute, details.second, details.millis);
	}
	static long calcMillis(int year, int month, final int day, final int hour, final int minute, final int second, final int millis){
		long result = millis;
		result += second * 1000;
		result += minute * 60000;
		result += hour * 3600000;
		result += (day-1) * 86400000L;
		if(month > 11){
			year += month / 12;
			month %= 12;
		}
		result += MONTH_DAYS[month] * 86400000L;
		result += (year - 1970) * 31536000000L; 
		result += ((year/4) - (year/100) + (year/400) - 477) * 86400000L;
		if(month<2 && year % 4 == 0 && (year%100 != 0 || year%400 == 0))
			result -= 86400000L;
		return result;
	}
	static long now(){	
		return removeDateTimeOffset( System.currentTimeMillis() );
	}
	static int dayOfWeek(long time){
		return (int)((time / 86400000 + 3) % 7);
	}
	static long parse(java.util.Date date){
		long t = date.getTime();
		return removeDateTimeOffset(t);
	}
	static DateTime valueOf(java.util.Date date){
		if(date == null) return null;
		int type;
		if(date instanceof java.sql.Date)
			type = SQLTokenizer.DATE;
		else
		if(date instanceof java.sql.Time)
			type = SQLTokenizer.TIME;
		else
			type = SQLTokenizer.TIMESTAMP;
		return new DateTime( parse(date), type);
	}
	static DateTime valueOf(java.sql.Date date){
		if(date == null) return null;
		return new DateTime( parse(date), SQLTokenizer.DATE);
	}
	static DateTime valueOf(java.sql.Time date){
		if(date == null) return null;
		return new DateTime( parse(date), SQLTokenizer.TIME);
	}
	static DateTime valueOf(java.sql.Timestamp date){
		if(date == null) return null;
		return new DateTime( parse(date), SQLTokenizer.TIMESTAMP);
	}
	static DateTime valueOf(String date, int dataType) throws SQLException{
		if(date == null) return null;
		return new DateTime( parse(date), dataType);
	}
	static long parse(final String datetime) throws SQLException{
		try{
			final int length = datetime.length();
			final int year;
			final int month;
			final int day;
			final int hour;
			final int minute;
			final int second;
			final int millis;
			int idx1 = 0;
			int idx2 = datetime.indexOf('-');
			if(idx2 > 0){
				year = Integer.parseInt(datetime.substring(idx1, idx2).trim());
				idx1 = idx2+1;
				idx2 = datetime.indexOf('-', idx1);
				month = Integer.parseInt(datetime.substring(idx1, idx2).trim())-1;
				idx1 = idx2+1;
				idx2 = datetime.indexOf(' ', idx1);
				if(idx2 < 0) idx2 = datetime.length();
				day = Integer.parseInt(datetime.substring(idx1, idx2).trim());
			}else{
				year  = 1970;
				month = 0;
				day   = 1;
			}
			idx1 = idx2+1;
			idx2 = datetime.indexOf(':', idx1);
			if(idx2>0){
				hour = Integer.parseInt(datetime.substring(idx1, idx2).trim());
				idx1 = idx2+1;
				idx2 = datetime.indexOf(':', idx1);
				minute = Integer.parseInt(datetime.substring(idx1, idx2).trim());
				idx1 = idx2+1;
				idx2 = datetime.indexOf('.', idx1);
				if(idx2 < 0) idx2 = datetime.length();
				second = Integer.parseInt(datetime.substring(idx1, idx2).trim());
				idx1 = idx2+1;
				if(idx1 < length){
					String strMillis = datetime.substring(idx1).trim();
					switch(strMillis.length()){
						case 1:
							millis = Integer.parseInt(strMillis) * 100;
							break;
						case 2:
							millis = Integer.parseInt(strMillis) * 10;
							break;
						case 3:
							millis = Integer.parseInt(strMillis);
							break;
						default:
							millis = Integer.parseInt(strMillis.substring(0,3));
					}
				}else
					millis = 0;
			}else{
				hour   = 0;
				minute = 0;
				second = 0;
				millis = 0;				
			}
            if(idx1 == 0 && length > 0){
                throw SmallSQLException.create(Language.DATETIME_INVALID);
            }
            if(month >= 12){
                throw SmallSQLException.create(Language.MONTH_TOOLARGE, datetime );
            }
            if(day >= 32){
                throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
            }
            if(day == 31){
                switch(month){
                case 1:
                case 3:
                case 5:
                case 8:
                case 10:
                    throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
                }
            }
            if(month == 1){
                if(day == 30){
                    throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
                }
                if(day == 29){
                    if(!isLeapYear(year)){
                        throw SmallSQLException.create(Language.DAYS_TOOLARGE, datetime );
                    }
                }
            }
            if(hour >= 24){
                throw SmallSQLException.create(Language.HOURS_TOOLARGE, datetime );
            }
            if(minute >= 60){
                throw SmallSQLException.create(Language.MINUTES_TOOLARGE, datetime );
            }
            if(second >= 60){
                throw SmallSQLException.create(Language.SECS_TOOLARGE, datetime );
            }
            if(millis >= 1000){
                throw SmallSQLException.create(Language.MILLIS_TOOLARGE, datetime );
            }
			return calcMillis(year, month, day, hour, minute, second, millis);
        }catch(SQLException ex){
            throw ex;
		}catch(Throwable ex){
			throw SmallSQLException.createFromException(Language.DATETIME_INVALID, datetime, ex );
		}
	}
	long getTimeMillis(){
		return time;
	}
	int getDataType(){
		return dataType;
	}
	public String toString(){
		Details details = new Details(time);
		StringBuffer buf = new StringBuffer();
		if(dataType != SQLTokenizer.TIME){
			formatNumber( details.year,  4, buf );
			buf.append('-');
			formatNumber( details.month + 1, 2, buf );
			buf.append('-');
			formatNumber( details.day,   2, buf );
		}
		if(dataType != SQLTokenizer.DATE){
			if(buf.length() > 0) buf.append(' ');
			formatNumber( details.hour,  2, buf );
			buf.append(':');
			formatNumber( details.minute, 2, buf );
			buf.append(':');
			formatNumber( details.second, 2, buf );
		}
		switch(dataType){
        case SQLTokenizer.TIMESTAMP:
        case SQLTokenizer.SMALLDATETIME:
			buf.append('.');
			formatMillis( details.millis, buf );
		}
		return buf.toString();
	}
    public boolean equals(Object obj){
        if(!(obj instanceof DateTime)) return false;
        DateTime value = (DateTime)obj;
        return value.time == time && value.dataType == dataType;
    }
	String toString(int style){
		if(style < 0)
			return toString();
		Details details = new Details(time);
		StringBuffer buf = new StringBuffer();
		switch(style){
			case 0:
			case 100: 
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				formatNumber( details.year, 4, buf);
				buf.append(' ');
				formatHour12( details.hour, buf );
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append( details.hour < 12 ? "AM" : "PM" );
				return buf.toString();
			case 1:   
				formatNumber( details.month+1, 2, buf);
				buf.append('/');
				formatNumber( details.day, 2, buf);
				buf.append('/');
				formatNumber( details.year % 100, 2, buf);
				return buf.toString();
			case 101:   
				formatNumber( details.month+1, 2, buf);
				buf.append('/');
				formatNumber( details.day, 2, buf);
				buf.append('/');
				formatNumber( details.year, 4, buf);
				return buf.toString();
			case 2: 
				formatNumber( details.year % 100, 2, buf);
				buf.append('.');
				formatNumber( details.month+1, 2, buf);
				buf.append('.');
				formatNumber( details.day, 2, buf);
				return buf.toString();
			case 102: 
				formatNumber( details.year, 4, buf);
				buf.append('.');
				formatNumber( details.month+1, 2, buf);
				buf.append('.');
				formatNumber( details.day, 2, buf);
				return buf.toString();
			case 3: 
				formatNumber( details.day, 2, buf);
				buf.append('/');
				formatNumber( details.month+1, 2, buf);
				buf.append('/');
				formatNumber( details.year % 100, 2, buf);
				return buf.toString();
			case 103: 
				formatNumber( details.day, 2, buf);
				buf.append('/');
				formatNumber( details.month+1, 2, buf);
				buf.append('/');
				formatNumber( details.year, 4, buf);
				return buf.toString();
			case 4: 
				formatNumber( details.day, 2, buf);
				buf.append('.');
				formatNumber( details.month+1, 2, buf);
				buf.append('.');
				formatNumber( details.year % 100, 2, buf);
				return buf.toString();
			case 104: 
				formatNumber( details.day, 2, buf);
				buf.append('.');
				formatNumber( details.month+1, 2, buf);
				buf.append('.');
				formatNumber( details.year, 4, buf);
				return buf.toString();
			case 5: 
				formatNumber( details.day, 2, buf);
				buf.append('-');
				formatNumber( details.month+1, 2, buf);
				buf.append('-');
				formatNumber( details.year % 100, 2, buf);
				return buf.toString();
			case 105: 
				formatNumber( details.day, 2, buf);
				buf.append('-');
				formatNumber( details.month+1, 2, buf);
				buf.append('-');
				formatNumber( details.year, 4, buf);
				return buf.toString();
			case 6: 
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.year % 100, 2, buf);
				return buf.toString();
			case 106: 
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.year, 4, buf);
				return buf.toString();
			case 7: 
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.day, 2, buf);
				buf.append(',');
				buf.append(' ');
				formatNumber( details.year % 100, 2, buf);
				return buf.toString();
			case 107: 
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.day, 2, buf);
				buf.append(',');
				buf.append(' ');
				formatNumber( details.year, 4, buf);
				return buf.toString();
			case 8: 
			case 108:
				formatNumber( details.hour, 2, buf);
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				return buf.toString();
			case 9:
			case 109: 
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				formatNumber( details.year, 4, buf);
				buf.append(' ');
				formatHour12( details.hour, buf );
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				buf.append(':');
				formatMillis( details.millis, buf);
				buf.append( details.hour < 12 ? "AM" : "PM" );
				return buf.toString();
			case 10: 
				formatNumber( details.month+1, 2, buf);
				buf.append('-');
				formatNumber( details.day, 2, buf);
				buf.append('-');
				formatNumber( details.year % 100, 2, buf);
				return buf.toString();
			case 110: 
				formatNumber( details.month+1, 2, buf);
				buf.append('-');
				formatNumber( details.day, 2, buf);
				buf.append('-');
				formatNumber( details.year, 4, buf);
				return buf.toString();
			case 11: 
				formatNumber( details.year % 100, 2, buf);
				buf.append('/');
				formatNumber( details.month+1, 2, buf);
				buf.append('/');
				formatNumber( details.day, 2, buf);
				return buf.toString();
			case 111: 
				formatNumber( details.year, 4, buf);
				buf.append('/');
				formatNumber( details.month+1, 2, buf);
				buf.append('/');
				formatNumber( details.day, 2, buf);
				return buf.toString();
			case 12: 
				formatNumber( details.year % 100, 2, buf);
				formatNumber( details.month+1, 2, buf);
				formatNumber( details.day, 2, buf);
				return buf.toString();
			case 112: 
				formatNumber( details.year, 4, buf);
				formatNumber( details.month+1, 2, buf);
				formatNumber( details.day, 2, buf);
				return buf.toString();
			case 13:
			case 113: 
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.year, 4, buf);
				buf.append(' ');
				formatNumber( details.hour, 2, buf );
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				buf.append(':');
				formatMillis( details.millis, buf);
				return buf.toString();
			case 14:
			case 114: 
				formatNumber( details.hour, 2, buf);
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				buf.append(':');
				formatMillis( details.millis, buf );
				return buf.toString();
			case 20:
			case 120: 
				formatNumber( details.year, 4, buf);
				buf.append('-');
				formatNumber( details.month+1, 2, buf);
				buf.append('-');
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				formatNumber( details.hour, 2, buf);
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				return buf.toString();
			case 21:
			case 121: 
				formatNumber( details.year, 4, buf);
				buf.append('-');
				formatNumber( details.month+1, 2, buf);
				buf.append('-');
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				formatNumber( details.hour, 2, buf);
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				buf.append('.');
				formatMillis( details.millis, buf );
				return buf.toString();
			case 26:
			case 126: 
				formatNumber( details.year, 4, buf);
				buf.append('-');
				formatNumber( details.month+1, 2, buf);
				buf.append('-');
				formatNumber( details.day, 2, buf);
				buf.append('T');
				formatNumber( details.hour, 2, buf);
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				buf.append('.');
				formatMillis( details.millis, buf );
				return buf.toString();
			case 130: 
				formatNumber( details.day, 2, buf);
				buf.append(' ');
				buf.append( SHORT_MONTHS[ details.month ]);
				buf.append(' ');
				formatNumber( details.year, 4, buf);
				buf.append(' ');
				formatHour12( details.hour, buf );
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				buf.append(':');
				formatMillis( details.millis, buf);
				buf.append( details.hour < 12 ? "AM" : "PM" );
				return buf.toString();
			case 131: 
				formatNumber( details.day, 2, buf);
				buf.append('/');
				formatNumber( details.month+1, 2, buf);
				buf.append('/');
				formatNumber( details.year % 100, 2, buf);
				buf.append(' ');
				formatNumber( details.hour, 2, buf);
				buf.append(':');
				formatNumber( details.minute, 2, buf);
				buf.append(':');
				formatNumber( details.second, 2, buf);
				buf.append(':');
				formatMillis( details.millis, buf );
				return buf.toString();
			default:
				return toString();
		}
	}
	private final static void formatNumber(int value, int digitCount, StringBuffer buf){
		buf.setLength(buf.length() + digitCount);
		if(value < 0) value = - value;
		for(int i=1; i<=digitCount; i++){
			buf.setCharAt( buf.length()-i, Utils.digits[ value % 10 ] );
			value /= 10;
		}
	}
	private final static void formatMillis(int millis,  StringBuffer buf){
		buf.append(Utils.digits[ (millis / 100) % 10 ]);
		int value = millis % 100;
		if(value != 0){
			buf.append(Utils.digits[ value / 10 ]);
			value %= 10;
			if(value != 0)
				buf.append(Utils.digits[ value ]);
		}
	}
	private final static void formatHour12(int hour,  StringBuffer buf){
		hour %= 12;
		if(hour == 0) hour = 12;
		formatNumber( hour, 2, buf );
	}
	private final static long addDateTimeOffset(long datetime){
        return addDateTimeOffset( datetime, TimeZone.getDefault());
	}
    final static long addDateTimeOffset(long datetime, TimeZone timezone){
        int t = (int)(datetime % 86400000);
        int d = (int)(datetime / 86400000);
        if(t<0){
            t += 86400000;
            d--;
        }              
        int millis = t % 1000;
        t /= 1000;
        synchronized(cal){
            cal.setTimeZone( timezone );
            cal.set( 1970, 0, d+1, 0, 0, t );
            cal.set( Calendar.MILLISECOND, millis );
            return cal.getTimeInMillis();
        }
    }
	private static long removeDateTimeOffset(long datetime){
		synchronized(cal){
			cal.setTimeZone( TimeZone.getDefault() );
			cal.setTimeInMillis( datetime );
			return datetime + cal.get( Calendar.ZONE_OFFSET) + cal.get( Calendar.DST_OFFSET);
		}
	}
	static Timestamp getTimestamp(long time){
		return new Timestamp( DateTime.addDateTimeOffset(time) ); 
	}
	static Time getTime(long time){
		return new Time( DateTime.addDateTimeOffset(time) ); 
	}
	static Date getDate(long time){
		return new Date( DateTime.addDateTimeOffset(time) ); 
	}
	public Object getImmutableObject(){
		switch(dataType){
			case SQLTokenizer.DATE:
				return getDate( time );
			case SQLTokenizer.TIME:
				return getTime( time );
			default:
				return getTimestamp( time );
		}
	}
	static class Details{
		int year;
		int month;
		int dayofyear;
		int day;
		int hour;
		int minute;
		int second;
		int millis;
		Details(long time){
			int t = (int)(time % 86400000);
			int d = (int)(time / 86400000);
			if(t<0){
				t += 86400000;
				d--;
			}				
			millis = t % 1000;
			t /= 1000;
			second = t % 60;
			t /= 60;
			minute = t % 60;
			t /= 60;
			hour = t % 24;
			year = 1970 - (int)(t / 365.2425);
			boolean isLeap;
			do{
				isLeap = false;
				dayofyear = day = d - ((year - 1970)*365 + (year/4) - (year/100) + (year/400) - 477);
				if(isLeapYear(year)){
					if(day < 59){
						day++;
						isLeap = true;
					}
					dayofyear++;
				}
				if(day < 0){
					year--;
					continue;
				}else
				if(day >= 365){
					year++;
					continue;
				}
				break;
			}while(true);
			if(isLeap && day == 59){
				month = 1;
				day   = 29;
			}else{
				for(int m=11; m>=0; m--){
					if(MONTH_DAYS[m] <= day){
						month = m;
						day   = day - MONTH_DAYS[m] + 1;
						break;
					}
				}
			}
		}
	}
    static boolean isLeapYear(int year){
        return year % 4 == 0 && (year%100 != 0 || year%400 == 0);
    }
	private static final Calendar cal = Calendar.getInstance();
}