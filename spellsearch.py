from pyspark import SparkConf,SparkContext
from pyspark.shell import spark
import tkinter as tk
import tkinter.ttk as ttk

class Spellsearch:

    def __init__(self):
        self.init_spark()

        self.window = tk.Tk()
        self.window.title("Spell search")
        self.window.rowconfigure(1, weight=1)
        self.window.columnconfigure(1, weight=1)

        # ---------------- Spell level -----------------------
        self.frame3 = tk.Frame(self.window)
        self.frame3.grid(column=0, row=0, pady=5, columnspan=3)

        self.lbl_level = tk.Label(self.frame3, text="Spell level",
                             font=('Helvetica', 12, 'bold'))
        self.lbl_level.pack(side="left")

        self.lbl_minLevel = tk.Label(self.frame3, text="min:")
        self.lbl_minLevel.pack(side="left")
        self.combo_minLevel = ttk.Combobox(self.frame3, width=7)
        self.combo_minLevel['values'] = (1, 2, 3, 4, 5, 6, 7, 8, 9)
        self.combo_minLevel.pack(side="left")
        self.combo_minLevel.current(0)

        self.lbl_maxLevel = tk.Label(self.frame3, text="max:")
        self.lbl_maxLevel.pack(side="left")
        self.combo_maxLevel = ttk.Combobox(self.frame3, width=7)
        self.combo_maxLevel['values'] = (1, 2, 3, 4, 5, 6, 7, 8, 9)
        self.combo_maxLevel.pack(side="left")
        self.combo_maxLevel.current()

        # ---------------- Classes -----------------------
        self.lbl_classes = tk.Label(self.window, text="Classes",
                               font=('Helvetica', 12, 'bold'))
        self.lbl_classes.grid(column=0, row=2, pady=5)

        self.frame = tk.Frame(self.window)
        self.frame.grid(column=0, row=3, pady=5, padx=(15, 0))

        self.classes = tk.Listbox(self.frame, selectmode="multiple", height=8, exportselection=0)
        self.classes.pack(side="left", fill="y")

        self.classes_names = ["adept","alchemist","antipaladin","arcanist","bard",
                              "bloodrager","cleric","druid","hunter","inquisitor",
                              "investigator","magus","medium","mesmerist","occultist",
                              "oracle","paladin","psychic","ranger","shaman","skald",
                              "sorcerer","spiritualist","summoner","warpriest","witch",
                              "wizard"] #"red Mantis Assassin", "sahir - Afiyun"

        for i in range(len(self.classes_names)):
            self.classes.insert(i + 1, self.classes_names[i])

        self.scrollbar = tk.Scrollbar(self.frame, orient="vertical")
        self.scrollbar.config(command=self.classes.yview)
        self.scrollbar.pack(side="right", fill="y")

        self.frame2 = tk.Frame(self.window)
        self.frame2.grid(column=0, row=4, pady=5, padx=(15, 0))

        self.switch_variable = tk.StringVar(value="OR")
        self.btn_or = tk.Radiobutton(self.frame2, text="Or", variable=self.switch_variable,
                                    indicatoron=False, value="OR", width=5)
        self.btn_and = tk.Radiobutton(self.frame2, text="And", variable=self.switch_variable,
                                    indicatoron=False, value="AND", width=5)
        self.btn_or.pack(side="left")
        self.btn_and.pack(side="left")

        # ---------------- Schools -----------------------
        self.lbl_schools = tk.Label(self.window, text="School",
                               font=('Helvetica', 12, 'bold'))
        self.lbl_schools.grid(column=1, row=2, pady=5)

        self.schools = tk.Listbox(self.window, selectmode="multiple", height=8, exportselection=0)
        self.schools.grid(column=1, row=3, pady=5, padx=(15,0))

        self.schools_names = ["abjuration", "Necromancy", "Enchantment", "Illusion",
                         "Divination", "Evocation", "Conjuration", "Transmutation"]

        for i in range(len(self.schools_names)):
            self.schools.insert(i + 1, self.schools_names[i])


        #---------------- Spell name -----------------------
        self.frame4 = tk.Frame(self.window)
        self.frame4.grid(column=0, row=5, pady=5, columnspan=4)

        self.lbl_name = tk.Label(self.frame4, text="Spell name",
                                  font=('Helvetica', 12, 'bold'))
        self.lbl_name.pack(side="left")

        self.entry_name = tk.Entry(self.frame4)
        self.entry_name.pack(side="left")

        self.switch_variable2 = tk.StringVar(value="begin")
        self.btn_begin = tk.Radiobutton(self.frame4, text="Begins with", variable=self.switch_variable2,
                                         indicatoron=False, value="begin", width=10)
        self.btn_end = tk.Radiobutton(self.frame4, text="Ends with", variable=self.switch_variable2,
                                         indicatoron=False, value="end", width=10)
        self.btn_anywhere = tk.Radiobutton(self.frame4, text="Anywhere", variable=self.switch_variable2,
                                         indicatoron=False, value="anywhere", width=10)
        self.btn_begin.pack(side="left")
        self.btn_end.pack(side="left")
        self.btn_anywhere.pack(side="left")



        self.btn_submit = ttk.Button(self.window, text="Submit", command=lambda: self.submit())
        self.btn_submit.grid(column=2, row=6, pady=10)
        self.window.mainloop()


    def init_spark(self):
        spark.conf.set("spark.sql.debug.maxToStringFields", 100)
        json_path = "spells.json"
        conf = SparkConf().setAppName("Test_RDD").setMaster("local")
        sc = SparkContext.getOrCreate(conf=conf)
        df_rdd = spark.read.json(json_path,multiLine=True)

        # On créé une vue
        df_rdd.createOrReplaceTempView("spells")

    def sqlRequest(self, request):
        # Requête SQL
        spellsRequest = spark.sql(request)
        spells = [str(row['name']) for row in spellsRequest.collect()]
        print(spells)


    def submit(self):
        classes_selection = list(map(lambda classe: self.classes_names[int(classe)], list(self.classes.curselection())))
        schools_selection = list(map(lambda school: self.schools_names[int(school)], list(self.schools.curselection())))
        level_min = self.combo_minLevel.get()
        level_max = self.combo_maxLevel.get()
        OR_or_AND = " " + self.switch_variable.get() + " "
        spell_name = self.entry_name.get()
        spell_name_where = self.switch_variable2.get()
        print(classes_selection, schools_selection, level_min, level_max, OR_or_AND, spell_name, spell_name_where)

        request = "SELECT name FROM spells WHERE "
        for i in range(len(classes_selection)):
            if i != 0:
                request += OR_or_AND

            request += "spells." + classes_selection[i] + " >= " + str(level_min)

            if level_max != "":
                request += " AND spells." + classes_selection[i] + " <= " + str(level_max)

        if spell_name != "":
            if len(classes_selection) > 0:
                request += " AND "
            where = ""
            if spell_name_where == "begin":
                request += "spells.name LIKE " + "\'" + spell_name + "%\'"
            elif spell_name_where == "end":
                request += "spells.name LIKE " + "\'%" + spell_name + "\'"
            elif spell_name_where == "anywhere":
                request += "spells.name LIKE " + "\'%" + spell_name + "%\'"





        #self.sqlRequest("SELECT name FROM spells WHERE spells.wizard < 5 AND spells.Components = 'V,'")
        print(request)
        self.sqlRequest(request)



if __name__ == '__main__':
    spellsearch = Spellsearch()
