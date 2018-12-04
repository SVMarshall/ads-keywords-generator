/*
   Copyright 2009 IBM Corp

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package deepmarketing.utils.cue.lang.stop;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import deepmarketing.utils.cue.lang.Counter;
import deepmarketing.utils.cue.lang.WordIterator;

/**
 * 
 * @author Jonathan Feinberg <jdf@us.ibm.com>
 * 
 */
public enum StopWords
{
	Arabic(), Catalan(true), Croatian(), Czech(), Dutch(), //
	Danish(), English(), Esperanto(), Farsi(), Finnish(), //
	French(true), German(), Greek(), Hindi(), Hungarian(), //
	Italian(), Latin(), Norwegian(), Polish(), Portuguese(), //
	Romanian(), Russian(), Slovenian(), Slovak(), Spanish(), //
	Swedish(), Hebrew(), Turkish(), Custom();

	public static StopWords guess(final String text)
	{
		return guess(new Counter<String>(new WordIterator(text)));
	}

	public static StopWords guess(final Counter<String> wordCounter)
	{
		return guess(wordCounter.getMostFrequent(50));
	}

	public static StopWords guess(final Collection<String> words)
	{
		StopWords currentWinner = null;
		int currentMax = 0;
		for (final StopWords stopWords : StopWords.values())
		{
			int count = 0;
			for (final String word : words)
			{
				if (stopWords.isStopWord(word))
				{
					count++;
				}
			}
			if (count > currentMax)
			{
				currentWinner = stopWords;
				currentMax = count;
			}
		}
		return currentWinner;
	}

	public final boolean stripApostrophes;
	private final Set<String> stopwords = new HashSet<String>();

	private StopWords()
	{
		this(false);
	}

	private StopWords(final boolean stripApostrophes)
	{
		this.stripApostrophes = stripApostrophes;
		loadLanguage();
	}

	public boolean isStopWord(final String s)
	{
		if (s.length() == 1)
		{
			return true;
		}
		// check rightquotes as apostrophes
		return stopwords.contains(s.replace('\u2019', '\'').toLowerCase(Locale.ENGLISH));
	}

	private void loadLanguage()
	{
		//final String wordlistResource = name().toLowerCase(Locale.ENGLISH);
		//if (!wordlistResource.equals("custom"))
		//{
		//	readStopWords(getClass().getResourceAsStream(wordlistResource), Charset.forName("UTF-8"));
		//}
		Collection<String> stopWords = Lists.newArrayList(
				"adonde",
				"adonde",
				"algo",
				"alguien",
				"alguna",
				"algunas",
				"alguno",
				"algunos",
				"ambas",
				"ambos",
				"aquel",
				"aquel",
				"aquella",
				"aquella",
				"aquellas",
				"aquellas",
				"aquello",
				"aquellos",
				"aquellos",
				"bastante",
				"bastantes",
				"como",
				"como",
				"conmigo",
				"consigo",
				"contigo",
				"cual",
				"cual",
				"cual",
				"cuales",
				"cuales",
				"cualesquiera",
				"cualquiera",
				"cuando",
				"cuando",
				"cuanta",
				"cuanta",
				"cuantas",
				"cuantas",
				"cuanto",
				"cuanto",
				"cuantos",
				"cuantos",
				"cuya",
				"cuyas",
				"cuyo",
				"cuyos",
				"demas",
				"demasiada",
				"demasiadas",
				"demasiado",
				"demasiados",
				"donde",
				"donde",
				"el",
				"ella",
				"ellas",
				"ello",
				"ellos",
				"esa",
				"esa",
				"esas",
				"esas",
				"ese",
				"ese",
				"eso",
				"esos",
				"esos",
				"esta",
				"esta",
				"estas",
				"estas",
				"este",
				"este",
				"esto",
				"estos",
				"estos",
				"estotra",
				"estotro",
				"idem",
				"idem",
				"la",
				"las",
				"le",
				"les",
				"lo",
				"lo",
				"los",
				"me",
				"media",
				"medias",
				"medio",
				"medios",
				"mi",
				"misma",
				"mismas",
				"mismo",
				"mismos",
				"mucha",
				"muchas",
				"mucho",
				"muchos",
				"nada",
				"nadie",
				"ninguna",
				"ningunas",
				"ninguno",
				"ningunos",
				"nos",
				"nosotras",
				"nosotros",
				"os",
				"otra",
				"otras",
				"otro",
				"otros",
				"poca",
				"pocas",
				"poco",
				"pocos",
				"que",
				"que",
				"que",
				"quien",
				"quien",
				"quienes",
				"quienes",
				"quienesquiera",
				"quienquier",
				"quienquiera",
				"se",
				"si",
				"tal",
				"tales",
				"tanta",
				"tantas",
				"tanto",
				"tantos",
				"te",
				"ti",
				"toda",
				"todas",
				"todoodosu",
				"una",
				"unas",
				"uno",
				"unos",
				"usted",
				"ustedes",
				"varias",
				"varios",
				"vos",
				"vosotras",
				"vosotros",
				"yo",
				"algun",
				"alguna",
				"algunas",
				"alguno",
				"algunos",
				"ambas",
				"ambos",
				"aquel",
				"aquella",
				"aquellas",
				"aquellos",
				"bastante",
				"bastantes",
				"cada",
				"cierta",
				"ciertas",
				"cierto",
				"ciertos",
				"cual",
				"cualesquier",
				"cualesquiera",
				"cualquier",
				"cualquiera",
				"cuanta",
				"cuanta",
				"cuanta",
				"cuantas",
				"cuantas",
				"cuantas",
				"cuanto",
				"cuanto",
				"cuanto",
				"cuantos",
				"cuantos",
				"cuantos",
				"demas",
				"demasiada",
				"demasiadas",
				"demasiado",
				"demasiados",
				"diferentes",
				"distintas",
				"distintos",
				"diversas",
				"diversos",
				"el",
				"esa",
				"esas",
				"escasa",
				"escasas",
				"escasisima",
				"escasisimas",
				"escasisimo",
				"escasisimos",
				"escaso",
				"escasos",
				"ese",
				"esos",
				"esta",
				"estas",
				"este",
				"estos",
				"la",
				"las",
				"lo",
				"los",
				"media",
				"medias",
				"medio",
				"medios",
				"mi",
				"mia",
				"mias",
				"mio",
				"mios",
				"mis",
				"misma",
				"mismas",
				"mismisima",
				"mismisimas",
				"mismisimo",
				"mismisimos",
				"mismo",
				"mismos",
				"mucha",
				"muchas",
				"mucho",
				"muchos",
				"ningun",
				"ninguna",
				"ningunas",
				"ninguno",
				"ningunos",
				"nuestra",
				"nuestras",
				"nuestro",
				"nuestros",
				"otra",
				"otras",
				"otro",
				"otros",
				"poca",
				"pocas",
				"poco",
				"pocos",
				"propia",
				"propias",
				"propio",
				"propios",
				"que",
				"que",
				"semejante",
				"semejantes",
				"sendas",
				"sendos",
				"su",
				"sus",
				"suya",
				"suyas",
				"suyo",
				"suyos",
				"tal",
				"tales",
				"tanta",
				"tantas",
				"tanto",
				"tantos",
				"toda",
				"todas",
				"todo",
				"todos",
				"tu",
				"tus",
				"tuya",
				"tuyas",
				"tuyo",
				"tuyos",
				"un",
				"una",
				"unas",
				"unos",
				"varias",
				"varios",
				"vuestra",
				"vuestras",
				"vuestro",
				"vuestros",
				"al",
				"del",
				"a",
				"con"
		);

		stopwords.addAll(stopWords);
	}

	public void readStopWords(final InputStream inputStream, final Charset encoding)
	{
		try
		{
			final BufferedReader in = new BufferedReader(new InputStreamReader(
					inputStream, encoding));
			try
			{
				String line;
				while ((line = in.readLine()) != null)
				{
					line = line.replaceAll("\\|.*", "").trim();
					if (line.length() == 0)
					{
						continue;
					}
					for (final String w : line.split("\\s+"))
					{
						stopwords.add(w.toLowerCase(Locale.ENGLISH));
					}
				}
			}
			finally
			{
				in.close();
			}
		}
		catch (final IOException e)
		{
			throw new RuntimeException(e);
		}
	}
}
