from pyspark import SparkConf,SparkContext
from pyspark.shell import spark
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import scrolledtext
from PIL import ImageTk, Image

class Spellsearch:

    def __init__(self):
        self.init_spark()

        self.window = tk.Tk()
        self.window.title("Spell search")
        self.window.rowconfigure(1, weight=1)
        self.window.columnconfigure(1, weight=1)

        self.mainframe = tk.Frame(self.window)
        self.mainframe.grid(column=0, row=0, pady=5, columnspan=1)
        # ---------------- Spell level -----------------------
        self.frame3 = tk.Frame(self.mainframe)
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
        self.lbl_classes = tk.Label(self.mainframe, text="Classes",
                               font=('Helvetica', 12, 'bold'))
        self.lbl_classes.grid(column=1, row=2, pady=5)

        self.frame = tk.Frame(self.mainframe)
        self.frame.grid(column=1, row=3, pady=5, padx=(15, 0))

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

        self.frame2 = tk.Frame(self.mainframe)
        self.frame2.grid(column=1, row=4, pady=5, padx=(15, 0))

        self.switch_variable = tk.StringVar(value="OR")
        self.btn_or = tk.Radiobutton(self.frame2, text="Or", variable=self.switch_variable,
                                    indicatoron=False, value="OR", width=5)
        self.btn_and = tk.Radiobutton(self.frame2, text="And", variable=self.switch_variable,
                                    indicatoron=False, value="AND", width=5)
        self.btn_or.pack(side="left")
        self.btn_and.pack(side="left")

        #---------------- Spell name -----------------------
        self.frame4 = tk.Frame(self.mainframe)
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

        # ---------------- VSM -----------------------

        self.frame5 = tk.Frame(self.mainframe)
        self.frame5.grid(column=0, row=6, pady=5, columnspan=4)

        self.lbl_vsm = tk.Label(self.mainframe, text="VSM",
                                font=('Helvetica', 12, 'bold'))
        self.lbl_vsm.grid(column=0, row=6)

        self.switch_variable3 = tk.StringVar(value="")

        self.check_v_btn = tk.Checkbutton(self.frame5, text="v", relief="raised", onvalue="V,", offvalue="", height=1,
                                          width=1, variable=self.switch_variable3,
                                          indicatoron=False)
        self.check_v_btn.pack(side="left")

        self.switch_variable4 = tk.StringVar(value="")

        self.check_s_btn = tk.Checkbutton(self.frame5, text="s", relief="raised", onvalue="S,", offvalue="", width=1,
                                          height=1, variable=self.switch_variable4,
                                          indicatoron=False)
        self.check_s_btn.pack(side="left")

        self.switch_variable5 = tk.StringVar(value="")

        self.check_m_btn = tk.Checkbutton(self.frame5, text="m", relief="raised", onvalue="M,", offvalue="", width=1,
                                          height=1, variable=self.switch_variable5,
                                          indicatoron=False)
        self.check_m_btn.pack(side="left")

        # ---------------- Results -----------------------
        self.frame6 = tk.Frame(self.window)
        self.frame6.grid(column=5, row=0, pady=5, columnspan=1)

        self.lbl_resultsTitle = tk.Label(self.frame6, text="Results",
                                  font=('Helvetica', 12, 'bold'))
        self.lbl_resultsTitle.pack(side="top")
        self.resultsBox = tk.Listbox(self.frame6, height=8, exportselection=0)
        self.resultsBox.pack(side="left")
        self.results = []

        self.scrollbar2 = tk.Scrollbar(self.frame6, orient="vertical")
        self.scrollbar2.config(command=self.resultsBox.yview)
        self.scrollbar2.pack(side="left", fill="y")

        self.btn_show_img = ttk.Button(self.frame6, text="Show Image", command=lambda: self.loadImage())
        self.btn_show_img.pack(side="right")

        # -------------------- Image ----------------------
        self.frame8 = tk.Frame(self.window)
        self.frame8.grid(column=6, row=0, pady=5, columnspan=1)

        self.panel = tk.Label(self.frame8)
        self.panel.pack(side="top")


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
        self.spells = [str(row['name']) for row in spellsRequest.collect()]
        print(self.spells)

    def loadImage(self):
        try:
            name = self.results[self.resultsBox.curselection()[0]]
            path = "img/" + name.lower() + ".png"
        except IndexError:
            im = Image.open("img/not_found.png")

        try:
            im = Image.open(path)
        except (FileNotFoundError, UnboundLocalError):
            im = Image.open("img/not_found.png")

        img = ImageTk.PhotoImage(im.resize((200, 300), Image.ANTIALIAS))
        self.panel.configure(image=img)
        self.panel.image = img

    def submit(self):
        classes_selection = list(map(lambda classe: self.classes_names[int(classe)], list(self.classes.curselection())))

        v_value = self.switch_variable3.get()
        s_value = self.switch_variable4.get()
        m_value = self.switch_variable5.get()

        level_min = self.combo_minLevel.get()
        level_max = self.combo_maxLevel.get()
        OR_or_AND = " " + self.switch_variable.get() + " "
        spell_name = self.entry_name.get()
        spell_name_where = self.switch_variable2.get()
        print(classes_selection, level_min, level_max, OR_or_AND, spell_name, spell_name_where)

        request = "SELECT name FROM spells WHERE "

        if len(classes_selection) != 0:
            request += "("

        for i in range(len(classes_selection)):
            if i != 0:
                request += OR_or_AND

            request += "spells." + classes_selection[i] + " >= " + str(level_min)

            if level_max != "":
                request += " AND spells." + classes_selection[i] + " <= " + str(level_max)

        if len(classes_selection) != 0:
            request += ")"

        if spell_name != "":
            if len(classes_selection) > 0:
                request += " AND ("
            else :
                request += "("

            if spell_name_where == "begin":
                request += "spells.name LIKE " + "\'" + spell_name + "%\'"
            elif spell_name_where == "end":
                request += "spells.name LIKE " + "\'%" + spell_name + "\'"
            elif spell_name_where == "anywhere":
                request += "spells.name LIKE " + "\'%" + spell_name + "%\'"
            request += ")"

        if (v_value != "") or (s_value != "") or (m_value != ""):

            if (len(classes_selection) > 0) or (spell_name != ""):
                request += " AND "

            is_there_a_and = 0
            if v_value != "" :
                is_there_a_and = 1
                request += "(spells.Components LIKE \'%" + v_value + "%\')"

            if s_value != "":
                if is_there_a_and == 1:
                    request += " AND "
                is_there_a_and = 1
                request += "(spells.Components LIKE \'%" + s_value + "%\')"

            if m_value != "":
                if is_there_a_and == 1:
                    request += " AND "
                request += "(spells.Components LIKE \'%" + m_value + "%\')"

        print(request)
        self.sqlRequest(request)

        self.displayResults()

    def displayResults(self):
        self.resultsBox.delete(0, tk.END)
        self.results = []
        i=1
        for spell in self.spells:
            self.resultsBox.insert(i, spell)
            self.results.append(spell)
            i+=1


if __name__ == '__main__':
    spellsearch = Spellsearch()
